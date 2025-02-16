package com.tmsvr.databases.lsmtree.sstable.index;

import com.tmsvr.databases.lsmtree.util.FileUtils;
import com.tmsvr.databases.serde.SerDe;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;

import static com.tmsvr.databases.lsmtree.sstable.LsmSerDe.SEPARATOR;

@Slf4j
public class Index<K extends Comparable<K>> {
    private static final String INDEX_FILE_SUFFIX = ".index";

    private final Path indexFile;
    private final SerDe<K> keySerDe;
    private final Map<K, Long> index;

    public Index(String filename, SerDe<K> keySerDe) throws IOException {
        this.indexFile = Paths.get(filename + INDEX_FILE_SUFFIX);
        this.keySerDe = keySerDe;
        this.index = loadFromDisk();
    }

    public int getSize() {
        return index.size();
    }

    public boolean exists() {
        return Files.exists(indexFile);
    }

    public Long getOffset(K key) {
        return index.get(key);
    }

    public void add(K key, long offset) {
        index.put(key, offset);
    }

    public void saveToDisk() throws IOException {
        FileUtils.saveToDisk(indexFile, index, x -> {
            StringBuilder csvBuilder = new StringBuilder();
            for (Map.Entry<K, Long> entry : x.entrySet()) {
                csvBuilder.append(keySerDe.serialize(entry.getKey()))
                        .append(SEPARATOR)
                        .append(entry.getValue())
                        .append(System.lineSeparator());
            }

            return csvBuilder.toString();
        });
    }

    private Map<K, Long> loadFromDisk() throws IOException {
        return FileUtils.loadFromDisk(indexFile, indexContent -> {
            Map<K, Long> map = new TreeMap<>();
            String[] lines = indexContent.split(System.lineSeparator());

            for (String line : lines) {
                String[] parts = line.split(SEPARATOR);
                if (parts.length == 2) {
                    map.put(keySerDe.deserialize(parts[0]), Long.parseLong(parts[1]));
                }
            }
            return map;
        }, new TreeMap<>());
    }
}

