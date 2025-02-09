package com.tmsvr.databases.lsmtree.sstable;

import com.tmsvr.databases.serde.SerDe;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

@Slf4j
public class SSTableManager<K extends Comparable<K>, V> {
    private static final int COMPACTION_THRESHOLD = 5;
    private final List<SSTable<K, V>> ssTables;

    private final Compactor<K, V> compactor;
    private final SerDe<K> keySerDe;
    private final SerDe<V> valueSerDe;

    private int newTablesSinceLastCompaction = 0;

    public SSTableManager(LsmSerDe<K> keySerDe, LsmSerDe<V> valueSerDe) {
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;

        this.ssTables = new ArrayList<>();
        this.compactor = new RowCountBasedCompactor<>(10);
    }

    public void flush(Map<K, V> data) throws IOException {
        SSTable<K, V> ssTable = new SSTable<>("sstable-" + UUID.randomUUID(), keySerDe, valueSerDe);
        ssTable.write(data);
        ssTables.add(ssTable);

        newTablesSinceLastCompaction++;

        if (newTablesSinceLastCompaction > COMPACTION_THRESHOLD) {
            compact();
        }
        System.out.println();
    }

    public Optional<V> findValue(K key) throws IOException {
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            Optional<V> value = ssTables.get(i).getValue(key);
            if (value.isPresent()) {
                return value;
            }
        }

        return Optional.empty();
    }

    public void readTablesFromFile() throws IOException {
        Path rootPath = Path.of("");

        try (Stream<Path> paths = Files.find(rootPath, 1, (path, _) -> path.toString().endsWith(".index"))) {
            paths.forEach(path -> {
                log.info("SSTable found: {}", path.toString().replace(".index", ""));
                try {
                    ssTables.add(new SSTable<>(path.toString().replace(".index", ""), keySerDe, valueSerDe));
                } catch (IOException e) {
                    log.warn("Error during reading tables from disk", e);
                }
            });
        }
    }

    public void compact() throws IOException {
        List<SSTable<K, V>> compactedTables = compactor.compact(ssTables);
        ssTables.clear();
        ssTables.addAll(compactedTables);

        newTablesSinceLastCompaction = 0;
    }
}
