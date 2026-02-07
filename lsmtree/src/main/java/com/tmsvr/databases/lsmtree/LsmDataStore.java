package com.tmsvr.databases.lsmtree;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.DataStore;
import com.tmsvr.databases.lsmtree.commitlog.CommitLog;
import com.tmsvr.databases.lsmtree.commitlog.CommitLogSegment;
import com.tmsvr.databases.lsmtree.commitlog.DefaultCommitLog;
import com.tmsvr.databases.lsmtree.memtable.ImmutableMemtable;
import com.tmsvr.databases.lsmtree.memtable.Memtable;
import com.tmsvr.databases.lsmtree.sstable.LsmSerDe;
import com.tmsvr.databases.lsmtree.sstable.SSTableManager;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class LsmDataStore<K extends Comparable<K>, V> implements DataStore<K, V>, AutoCloseable {
    private static final int MAX_IMMUTABLE_MEMTABLES = 4; // Backpressure limit
    public static int FLUSH_TO_DISK_LIMIT = 5;

    private final AtomicReference<Memtable<K, V>> activeMemtable;
    private final Deque<ImmutableMemtable<K, V>> immutableMemtables;

    private final ReadWriteLock rotationLock = new ReentrantReadWriteLock();

    private final ExecutorService flushExecutor;

    private final CommitLog<K, V> commitLog;
    private final SSTableManager<K, V> ssTableManager;

    public LsmDataStore(LsmSerDe<K> keySerDe, LsmSerDe<V> valueSerDe, int memtableSize) throws IOException {
        FLUSH_TO_DISK_LIMIT = memtableSize;
        this.commitLog = new DefaultCommitLog<>(keySerDe, valueSerDe);
        this.ssTableManager = new SSTableManager<>(keySerDe, valueSerDe);

        // Initialize executors
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "lsm-flush");
            t.setDaemon(true);
            return t;
        });

        this.immutableMemtables = new ConcurrentLinkedDeque<>();

        // Crash recovery: recover segments and populate initial memtable
        var segments = commitLog.recoverSegments();
        if (!segments.isEmpty()) {
            log.info("Recovering from {} commit log segments", segments.size());
            // For now, create active memtable with first segment, queue others for flush
            CommitLogSegment<K, V> activeSegment = segments.get(0);
            this.activeMemtable = new AtomicReference<>(new Memtable<>(activeSegment));

            // Queue remaining segments as immutable (they'll be flushed on next write)
            for (int i = 1; i < segments.size(); i++) {
                Memtable<K, V> recovered = new Memtable<>(segments.get(i));

                ImmutableMemtable<K, V> imm = recovered.freeze();
                immutableMemtables.addFirst(imm);
                flushExecutor.submit(() -> flushImmutableMemtable(imm));
            }
        } else {
            // No recovery needed - create fresh segment
            CommitLogSegment<K, V> initialSegment = commitLog.createSegment();
            this.activeMemtable = new AtomicReference<>(new Memtable<>(initialSegment));
        }

        // Read existing SSTables from disk
        ssTableManager.readTablesFromFile();
    }

    // Package-private constructor for testing
    LsmDataStore(CommitLog<K, V> commitLog, Memtable<K, V> memtable, SSTableManager<K, V> ssTableManager) {
        this.commitLog = commitLog;
        this.activeMemtable = new AtomicReference<>(memtable);
        this.ssTableManager = ssTableManager;
        this.immutableMemtables = new ConcurrentLinkedDeque<>();
        this.flushExecutor = Executors.newSingleThreadExecutor();
    }

    /**
     * Write path - non-blocking.
     * Writers never wait for flush or compaction.
     */
    @Override
    public void put(K key, V value) throws IOException {
        DataRecord<K, V> record = new DataRecord<>(key, value);

        rotationLock.readLock().lock();
        Memtable<K, V> current;
        try {
            current = activeMemtable.get();
            current.put(record);
        } finally {
            rotationLock.readLock().unlock();
        }

        if (current.getSize() > FLUSH_TO_DISK_LIMIT) {
            maybeRotateMemtable(current);
        }
    }

    /**
     * Trigger rotation if current memtable matches activeMemtable and is over limit.
     * Uses write lock to ensure no writes are in progress during swap.
     */
    private void maybeRotateMemtable(Memtable<K, V> activeMemtable) throws IOException {
        rotationLock.writeLock().lock();
        try {
            // Re-check state after acquiring write lock (not rotated yet && size indicates rotation)
            if (this.activeMemtable.get() == activeMemtable && activeMemtable.getSize() > FLUSH_TO_DISK_LIMIT) {
                rotateMemtable(activeMemtable);
            }
        } finally {
            rotationLock.writeLock().unlock();
        }
    }

    /**
     * Read path - check all sources (READ VISIBILITY INVARIANT).
     * INVARIANT: Reads must see all data in:
     * 1. Active memtable (most recent)
     * 2. All immutable memtables (head→tail is newest→oldest)
     * 3. SSTables (reverse order)
     * Never skip immutable memtables, even if flush has started.
     */
    @Override
    public Optional<V> get(K key) {
        // 1. Check active memtable
        V value = activeMemtable.get().get(key);
        if (value != null) {
            return Optional.of(value);
        }

        // 2. Check immutable memtables (head = newest)
        for (ImmutableMemtable<K, V> imm : immutableMemtables) {
            value = imm.get(key);
            if (value != null) {
                return Optional.of(value);
            }
        }

        // 3. Check SSTables
        try {
            return ssTableManager.findValue(key);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read from SSTables", e);
        }
    }

    @Override
    public void delete(K key) throws IOException {
        put(key, null); // Tombstone
    }

    /**
     * Memtable rotation implementation.
     * MUST be called while holding write lock.
     */
    private void rotateMemtable(Memtable<K, V> memtable) throws IOException {
        // Backpressure: check immutable memtable count
        if (immutableMemtables.size() >= MAX_IMMUTABLE_MEMTABLES) {
            log.warn("Too many pending immutable memtables ({}), applying backpressure", immutableMemtables.size());
        }

        // Create new memtable with new WAL segment (atomic pairing)
        CommitLogSegment<K, V> newSegment = commitLog.createSegment();
        Memtable<K, V> newMemtable = new Memtable<>(newSegment);

        // Atomic swap
        activeMemtable.set(newMemtable);

        log.debug("Rotated memtable, queuing for flush");

        // Freeze old memtable and submit for flush
        ImmutableMemtable<K, V> immutable = memtable.freeze();
        immutableMemtables.addFirst(immutable); // Add to head (newest)
        flushExecutor.submit(() -> flushImmutableMemtable(immutable));
    }

    /**
     * Background flush task - runs asynchronously.
     * Flushes immutable memtable to SSTable and deletes WAL segment.
     */
    private void flushImmutableMemtable(ImmutableMemtable<K, V> imm) {
        try {
            log.debug("Flushing immutable memtable with segment: {}", imm.getCommitLogSegmentId());

            ssTableManager.flush(imm);

            imm.deleteCommitLogSegment();
            immutableMemtables.remove(imm);

            log.debug("Successfully flushed and removed immutable memtable");
        } catch (IOException e) {
            log.error("Flush failed for segment: {}", imm.getCommitLogSegmentId(), e);
        }
    }

    /**
     * Graceful shutdown: flush all pending memtables and stop executors.
     */
    @Override
    public void close() throws Exception {
        log.info("Closing LsmDataStore - flushing pending memtables");

        // 1. Stop accepting new flush tasks
        flushExecutor.shutdown();

        // 2. Wait for in-flight flushes to complete
        if (!flushExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
            log.warn("Flush executor did not terminate, forcing shutdown");
            flushExecutor.shutdownNow();
        }

        // 3. Drain remaining immutable memtables synchronously
        while (!immutableMemtables.isEmpty()) {
            ImmutableMemtable<K, V> imm = immutableMemtables.pollFirst();
            if (imm != null) {
                flushImmutableMemtable(imm);
            }
        }

        // 4. Flush active memtable
        Memtable<K, V> active = activeMemtable.get();
        if (active.getSize() > 0) {
            ImmutableMemtable<K, V> imm = active.freeze();
            flushImmutableMemtable(imm);
        } else {
            // Active is empty, just clean up its segment
            active.freeze().deleteCommitLogSegment();
        }

        log.info("LsmDataStore closed successfully");
    }
}
