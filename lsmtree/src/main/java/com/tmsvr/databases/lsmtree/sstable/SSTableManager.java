package com.tmsvr.databases.lsmtree.sstable;

import com.tmsvr.databases.lsmtree.memtable.ImmutableMemtable;
import com.tmsvr.databases.serde.SerDe;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * SSTable manager with thread-safe immutable list for concurrent access.
 * Uses AtomicReference for lock-free reads and atomic publication of new
 * SSTables.
 */
@Slf4j
public class SSTableManager<K extends Comparable<K>, V> {
    private static final int COMPACTION_THRESHOLD = 5;

    private final AtomicReference<List<SSTable<K, V>>> ssTables;
    private final AtomicInteger newTablesSinceLastCompaction;

    private final Compactor<K, V> compactor;
    private final SerDe<K> keySerDe;
    private final SerDe<V> valueSerDe;

    public SSTableManager(LsmSerDe<K> keySerDe, LsmSerDe<V> valueSerDe) {
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;

        this.ssTables = new AtomicReference<>(Collections.emptyList());
        this.newTablesSinceLastCompaction = new AtomicInteger(0);
        this.compactor = new RowCountBasedCompactor<>(10);
    }

    /**
     * Flush immutable memtable to a new SSTable.
     * Atomically publishes the new SSTable to the immutable list.
     */
    public void flush(ImmutableMemtable<K, V> memtable) throws IOException {
        // Early exit if memtable is empty
        boolean empty = true;
        for (var ignored : memtable.entries()) {
            empty = false;
            break;
        }
        if (empty) {
            log.debug("Skipping flush of empty memtable");
            return;
        }

        // Create SSTable and write directly from immutable memtable
        SSTable<K, V> ssTable = new SSTable<>("sstable-" + UUID.randomUUID(), keySerDe, valueSerDe);

        ssTable.write(memtable.entries()); // ZERO-COPY

        // Atomically publish new SSTable
        ssTables.updateAndGet(current -> {
            List<SSTable<K, V>> updated = new ArrayList<>(current);
            updated.add(ssTable);
            return Collections.unmodifiableList(updated);
        });

        log.debug("Flushed SSTable: {}", ssTable.getName());

        int newCount = newTablesSinceLastCompaction.incrementAndGet();
        if (newCount > COMPACTION_THRESHOLD) {
            log.info("Compaction threshold reached, triggering compaction");
            compact();
        }
    }

    /**
     * Find value in SSTables by searching newest to oldest.
     * Uses snapshot read for thread-safety.
     */
    public Optional<V> findValue(K key) throws IOException {
        List<SSTable<K, V>> snapshot = ssTables.get(); // Atomic read

        // Search reverse order (newest first)
        for (int i = snapshot.size() - 1; i >= 0; i--) {
            Optional<V> value = snapshot.get(i).getValue(key);
            if (value.isPresent()) {
                return value;
            }
        }

        return Optional.empty();
    }

    /**
     * Read existing SSTables from disk into memory.
     */
    public void readTablesFromFile() throws IOException {
        List<SSTable<K, V>> tables = new ArrayList<>();
        Path rootPath = Path.of("");

        try (Stream<Path> paths = Files.find(rootPath, 1, (path, _) -> path.toString().endsWith(".index"))) {
            paths.forEach(path -> {
                log.info("SSTable found: {}", path.toString().replace(".index", ""));
                try {
                    tables.add(new SSTable<>(path.toString().replace(".index", ""), keySerDe, valueSerDe));
                } catch (IOException e) {
                    log.warn("Error during reading tables from disk", e);
                }
            });
        }

        if (!tables.isEmpty()) {
            ssTables.set(Collections.unmodifiableList(tables));
            log.info("Loaded {} SSTables from disk", tables.size());
        }
    }

    /**
     * Compact SSTables and atomically publish the result.
     */
    public void compact() throws IOException {
        List<SSTable<K, V>> snapshot = ssTables.get();

        if (snapshot.isEmpty()) {
            log.debug("No SSTables to compact");
            return;
        }

        log.info("Compacting {} SSTables", snapshot.size());
        List<SSTable<K, V>> compactedTables = compactor.compact(snapshot);

        // Atomic swap
        ssTables.set(Collections.unmodifiableList(compactedTables));
        newTablesSinceLastCompaction.set(0);

        log.info("Compaction complete, result: {} SSTables", compactedTables.size());
    }
}
