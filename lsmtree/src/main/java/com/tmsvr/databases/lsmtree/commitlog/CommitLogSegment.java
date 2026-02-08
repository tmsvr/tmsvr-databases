package com.tmsvr.databases.lsmtree.commitlog;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.serde.SerDe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.tmsvr.databases.lsmtree.sstable.LsmSerDe.SEPARATOR;

/**
 * Asynchronous commit log segment with group commit.
 *
 * Durability model:
 * - append() returns after enqueue (NOT fsync)
 * - data is flushed to disk periodically
 * - crash may lose last FLUSH_EVERY_MS or FLUSH_EVERY_N_RECORDS
 *
 * This matches RocksDB / Cassandra behavior.
 */
@Slf4j
public class CommitLogSegment<K extends Comparable<K>, V> {

    // Tunables
    private static final int FLUSH_EVERY_N_RECORDS = 2048;
    private static final long FLUSH_EVERY_MS = 10;
    private static final int QUEUE_CAPACITY = 100_000;

    @Getter
    private final String segmentId;
    private final SerDe<K> keySerDe;
    private final SerDe<V> valueSerDe;
    private final FileChannel fileChannel;
    private final Path filePath;

    private final BlockingQueue<DataRecord<K, V>> queue;
    private final Thread writerThread;
    private volatile boolean running = true;

    // Reused write buffer (writer thread only)
    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(1 << 20); // 1 MB

    public CommitLogSegment(String segmentId,
                            SerDe<K> keySerDe,
                            SerDe<V> valueSerDe) throws IOException {

        this.segmentId = segmentId;
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
        this.filePath = Paths.get("commit-log-" + segmentId + ".wal");

        this.fileChannel = FileChannel.open(
                filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );

        this.queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        this.writerThread = new Thread(this::runWriter, "commit-log-writer-" + segmentId);
        this.writerThread.start();

        log.debug("Created async commit log segment: {}", segmentId);
    }

    /**
     * Enqueue record for asynchronous WAL append.
     * Returns immediately after enqueue.
     */
    public void append(DataRecord<K, V> record) throws IOException {
        if (!running) {
            throw new IOException("Commit log segment is closed");
        }

        boolean offered = queue.offer(record);
        if (!offered) {
            // Backpressure – WAL is falling behind
            throw new IOException("Commit log queue full (backpressure)");
        }
    }

    private void runWriter() {
        List<DataRecord<K, V>> batch = new ArrayList<>(FLUSH_EVERY_N_RECORDS);
        long lastFlushTime = System.nanoTime();

        try {
            while (running || !queue.isEmpty()) {
                DataRecord<K, V> first = queue.poll(FLUSH_EVERY_MS, TimeUnit.MILLISECONDS);

                if (first != null) {
                    batch.add(first);
                    queue.drainTo(batch, FLUSH_EVERY_N_RECORDS - 1);
                }

                if (batch.isEmpty()) {
                    continue;
                }

                writeBatch(batch);
                batch.clear();

                long now = System.nanoTime();
                if (shouldFlush(now, lastFlushTime)) {
                    fileChannel.force(false); // data only
                    lastFlushTime = now;
                }
            }

            // Final flush on shutdown
            fileChannel.force(true);

        } catch (Exception e) {
            log.error("Commit log writer thread failed", e);
        }
    }

    private boolean shouldFlush(long now, long lastFlushTime) {
        if (writeBuffer.position() > writeBuffer.capacity() / 2) {
            return true;
        }
        return TimeUnit.NANOSECONDS.toMillis(now - lastFlushTime) >= FLUSH_EVERY_MS;
    }

    private void writeBatch(List<DataRecord<K, V>> batch) throws IOException {
        for (DataRecord<K, V> record : batch) {
            writeLine(record);
        }

        writeBuffer.flip();
        while (writeBuffer.hasRemaining()) {
            fileChannel.write(writeBuffer);
        }
        writeBuffer.clear();
    }

    private void writeLine(DataRecord<K, V> record) {
        String line =
                keySerDe.serialize(record.key()) +
                        SEPARATOR +
                        valueSerDe.serialize(record.value()) +
                        '\n';

        byte[] bytes = line.getBytes();

        if (bytes.length > writeBuffer.remaining()) {
            flushBuffer();
        }

        writeBuffer.put(bytes);
    }

    private void flushBuffer() {
        try {
            writeBuffer.flip();
            while (writeBuffer.hasRemaining()) {
                fileChannel.write(writeBuffer);
            }
            writeBuffer.clear();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush WAL buffer", e);
        }
    }

    public void close() throws IOException {
        if (!running) return;

        running = false;
        try {
            writerThread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        fileChannel.close();
        log.debug("Closed commit log segment: {}", segmentId);
    }

    public void delete() throws IOException {
        close();
        Files.deleteIfExists(filePath);
        log.debug("Deleted commit log segment: {}", segmentId);
    }
}
