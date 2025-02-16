package com.tmsvr.databases.lsmtree.sstable;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.lsmtree.sstable.bloomfilter.BloomFilter;
import com.tmsvr.databases.lsmtree.sstable.index.Index;
import com.tmsvr.databases.serde.SerDe;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.tmsvr.databases.lsmtree.LsmDataStore.FLUSH_TO_DISK_LIMIT;
import static com.tmsvr.databases.lsmtree.sstable.LsmSerDe.SEPARATOR;

@Slf4j
public class SSTable<K extends Comparable<K>, V> {
    private static final String DATA_FILE_SUFFIX = ".data";

    private final Path dataFile;
    private final SerDe<K> keySerDe;
    private final SerDe<V> valueSerDe;
    private final Index<K> index;
    private final BloomFilter<K> filter;

    public SSTable(String filename, SerDe<K> keySerDe, SerDe<V> valueSerDe) throws IOException {
        this.dataFile = Paths.get(filename + DATA_FILE_SUFFIX);
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
        this.index = new Index<>(filename, keySerDe);
        this.filter = new BloomFilter<>(FLUSH_TO_DISK_LIMIT, 0.01, filename);
    }

    public int getSize() {
        return index.getSize();
    }

    public SerDe<K> getKeySerDe() {
        return keySerDe;
    }

    public SerDe<V> getValueSerDe() {
        return valueSerDe;
    }

    public String getName() {
        return dataFile.getFileName().toString();
    }

    public void write(List<DataRecord<K, V>> records) throws IOException {
        Map<K, V> collected = records.stream().collect(Collectors.toMap(DataRecord::key, DataRecord::value, (v1, _) -> v1, TreeMap::new));
        this.write(collected);
    }

    public void write(Map<K, V> data) throws IOException {
        if (index.exists()) {
            log.warn("SSTable can't be written, Index file already exists");
            return;
        }

        Map<K, V> sortedData = (data instanceof TreeMap) ? data : new TreeMap<>(data);

        log.info("Writing SSTable to disk: {}", dataFile.getFileName().toString());

        try (BufferedWriter writer = Files.newBufferedWriter(dataFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            long offset = 0;

            for (Map.Entry<K, V> entry : sortedData.entrySet()) {
                String line = keySerDe.serialize(entry.getKey()) + SEPARATOR + valueSerDe.serialize(entry.getValue()) + System.lineSeparator();
                writer.write(line);

                filter.add(entry.getKey());
                index.add(entry.getKey(), offset);

                offset += line.getBytes(StandardCharsets.UTF_8).length;
            }
        }

        filter.saveToDisk();
        index.saveToDisk();
    }

    public Optional<V> getValue(K key) throws IOException {
        if (!filter.isPresent(key)) {
            return Optional.empty();
        }

        String stringKey = keySerDe.serialize(key);
        Long offset = index.getOffset(key);
        if (offset == null) {
            return Optional.empty();
        }

        try (RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "r")) {
            raf.seek(offset);

            String result = raf.readLine();
            if (result == null) {
                return Optional.empty();
            }

            String[] parts = result.split(SEPARATOR, 2);
            if (parts.length < 2) {
                return Optional.empty();
            }

            String foundKey = parts[0];
            if (!foundKey.equals(stringKey)) {
                throw new IllegalStateException("Unexpected key: " + foundKey);
            }

            return Optional.ofNullable(valueSerDe.deserialize(parts[1]));
        }
    }

    public List<DataRecord<K, V>> getAllLines() throws IOException {
        return Files.readAllLines(dataFile).stream()
                .map(line -> {
                    String[] parts = line.split(SEPARATOR, 2);
                    if (parts.length < 2) {
                        throw new IllegalArgumentException("Invalid input format: " + line);
                    }
                    K key = keySerDe.deserialize(parts[0]);
                    V value = valueSerDe.deserialize(parts[1]);
                    return new DataRecord<>(key, value);
                })
                .collect(Collectors.toList());
    }
}
