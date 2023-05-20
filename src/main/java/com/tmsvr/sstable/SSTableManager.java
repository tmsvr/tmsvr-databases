package com.tmsvr.sstable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class SSTableManager {
    private final List<SSTable> ssTables;

    private final Compactor compactor;

    public SSTableManager() {
        this.ssTables = new ArrayList<>();
        this.compactor = new RowCountBasedCompactor();
    }

    public void flush(Map<String, String> data) throws IOException {
        SSTable ssTable = new SSTable("sstable-" + ssTables.size());
        ssTable.write(data);
        ssTables.add(ssTable);
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
                System.out.println("SSTable found: " + path.toString().replace(".index", ""));
                try {
                    ssTables.add(new SSTable(path.toString().replace(".index", "")));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public void compact() {
        List<SSTable> compactedTables = compactor.compact(ssTables);



        ssTables.clear();
        ssTables.addAll(compactedTables);
    }
}
