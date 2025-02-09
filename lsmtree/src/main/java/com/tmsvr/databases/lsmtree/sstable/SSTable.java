package com.tmsvr.databases.lsmtree.sstable;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.serde.SerDe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tmsvr.databases.lsmtree.sstable.LsmSerDe.SEPARATOR;

@Slf4j
public class SSTable<K extends Comparable<K>, V> {
    private static final String INDEX_FILE_SUFFIX = ".index";
    private static final String DATA_FILE_SUFFIX = ".data";

    private final Path indexFile;
    private final Path dataFile;
    @Getter
    private final SerDe<K> keySerDe;
    @Getter
    private final SerDe<V> valueSerDe;
    private final Map<K, Long> index;

    public SSTable(String filename, SerDe<K> keySerDe, SerDe<V> valueSerDe) throws IOException {
        this.indexFile = Paths.get(filename + INDEX_FILE_SUFFIX);
        this.dataFile = Paths.get(filename + DATA_FILE_SUFFIX);
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
        this.index = loadIndex();
    }

    public int getSize() {
        return index.size();
    }

    public void write(List<DataRecord<K, V>> records) throws IOException {
        this.write(records.stream().collect(Collectors.toMap(DataRecord::key, DataRecord::value)));
    }

    public void write(Map<K, V> data) throws IOException {
        if (Files.exists(indexFile)) {
            log.warn("SSTable can't be written, Index file already exists");
            return;
        }

        Map<K, V> sortedData = new TreeMap<>(data);

        // Write data to a temporary file
        Path tempFile = Files.createFile(Path.of(dataFile.getFileName().toString() + ".tmp"));

        // Write the data to the temporary file and create an index
        Map<K, Long> newIndex = new TreeMap<>();
        long offset = 0;
        for (Map.Entry<K, V> entry : sortedData.entrySet()) {
            Files.write(tempFile, (keySerDe.serialize(entry.getKey()) + SEPARATOR + valueSerDe.serialize(entry.getValue()) + System.lineSeparator()).getBytes(), StandardOpenOption.APPEND);
            newIndex.put(entry.getKey(), offset);
            offset++;
        }

        // Write the index to the index file
        Files.createFile(indexFile);
        try (ObjectOutputStream indexObjOut = new ObjectOutputStream(new FileOutputStream(indexFile.toFile()))) {
            indexObjOut.writeObject(newIndex);
        }

        // Rename the temporary file to the data file
        Files.move(tempFile, dataFile);

        // Update the in-memory index
        index.clear();
        index.putAll(newIndex);
    }

    public Optional<V> getValue(K key) throws IOException {
        String stringKey = keySerDe.serialize(key);
        Long offset = index.get(key);
        if (offset == null) {
            return Optional.empty();
        }

        String result;
        try (Stream<String> lines = Files.lines(dataFile)) {
            Optional<String> firstLineAfterOffset = lines.skip(offset).findFirst();

            if (firstLineAfterOffset.isPresent()) {
                result = firstLineAfterOffset.get();
            } else {
                return Optional.empty();
            }
        }

        String foundKey = result.split(SEPARATOR)[0];

        if (!foundKey.equals(stringKey)) {
            throw new IllegalStateException("Unexpected key: " + foundKey);
        }
        return Optional.ofNullable(valueSerDe.deserialize(result.split(SEPARATOR)[1]));
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

    @SuppressWarnings("unchecked")
    private Map<K, Long> loadIndex() throws IOException {
        try (ObjectInputStream indexObjIn = new ObjectInputStream(new FileInputStream(indexFile.toFile()))) {
            return (Map<K, Long>) indexObjIn.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to load index", e);
        } catch (EOFException | FileNotFoundException e) {
            log.info("Index file is empty");
            return new TreeMap<>();
        }
    }
}
