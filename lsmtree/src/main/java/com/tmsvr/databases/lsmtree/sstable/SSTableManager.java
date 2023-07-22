package com.tmsvr.databases.lsmtree.sstable;

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
public class SSTableManager {
    private static final int COMPACTION_THRESHOLD = 5;
    private final List<SSTable> ssTables;

    private final Compactor compactor;

    private int newTablesSinceLastCompaction = 0;

    public SSTableManager() {
        this.ssTables = new ArrayList<>();
        this.compactor = new RowCountBasedCompactor(10);
    }

    public void flush(Map<String, String> data) throws IOException {
        SSTable ssTable = new SSTable("sstable-" + UUID.randomUUID());
        ssTable.write(data);
        ssTables.add(ssTable);

        newTablesSinceLastCompaction++;

        if (newTablesSinceLastCompaction > COMPACTION_THRESHOLD) {
            compact();
        }
    }

    public Optional<String> findValue(String key) throws IOException {
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            Optional<String> value = ssTables.get(i).getValue(key);
            if (value.isPresent()) {
                return value;
            }
        }

        return Optional.empty();
    }

    public void readTablesFromFile() throws IOException {
        Path rootPath = Path.of("");

        try (Stream<Path> paths = Files.find(rootPath, 1, (path, attr) -> path.toString().endsWith(".index"))) {
            paths.forEach(path -> {
                log.info("SSTable found: {}", path.toString().replace(".index", ""));
                try {
                    ssTables.add(new SSTable(path.toString().replace(".index", "")));
                } catch (IOException e) {
                    log.warn("Error during reading tables from disk", e);
                }
            });
        }
    }

    public void compact() throws IOException {
        List<SSTable> compactedTables = compactor.compact(ssTables);
        ssTables.clear();
        ssTables.addAll(compactedTables);

        newTablesSinceLastCompaction = 0;
    }
}
