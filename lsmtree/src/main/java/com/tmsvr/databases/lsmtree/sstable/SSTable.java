package com.tmsvr.databases.lsmtree.sstable;

import com.tmsvr.databases.DataRecord;
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

@Slf4j
public class SSTable {
    private static final String INDEX_FILE_SUFFIX = ".index";
    private static final String DATA_FILE_SUFFIX = ".data";

    private final Path indexFile;
    private final Path dataFile;
    private final Map<String, Long> index;

    public SSTable(String filename) throws IOException {
        this.indexFile = Paths.get(filename + INDEX_FILE_SUFFIX);
        this.dataFile = Paths.get(filename + DATA_FILE_SUFFIX);
        this.index = loadIndex();
    }

    public int getSize() {
        return index.size();
    }

    public void write(List<DataRecord> records) throws IOException {
        this.write(records.stream().collect(Collectors.toMap(DataRecord::key, DataRecord::value)));
    }

    public void write(Map<String, String> data) throws IOException {
        if (Files.exists(indexFile)) {
            log.warn("SSTable can't be written, Index file already exists");
            return;
        }

        Map<String, String> sortedData = new TreeMap<>(data);

        // Write data to a temporary file
        Path tempFile = Files.createFile(Path.of(dataFile.getFileName().toString() + ".tmp"));

        // Write the data to the temporary file and create an index
        Map<String, Long> newIndex = new TreeMap<>();
        long offset = 0;
        for (Map.Entry<String, String> entry : sortedData.entrySet()) {
            Files.write(tempFile, (entry.getKey() + "::" + entry.getValue() + System.lineSeparator()).getBytes(), StandardOpenOption.APPEND);
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

    public Optional<String> getValue(String key) throws IOException {
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

        String foundKey = result.split("::")[0];

        if (!foundKey.equals(key)) {
            throw new IllegalStateException("Unexpected key: " + foundKey);
        }
        return Optional.of(result.split("::")[1]);
    }

    List<DataRecord> getAllLines() throws IOException {
        return Files.readAllLines(dataFile).stream()
                .map(line -> new DataRecord(line.split("::")[0], line.split("::")[1]))
                .toList();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Long> loadIndex() throws IOException {
        try (ObjectInputStream indexObjIn = new ObjectInputStream(new FileInputStream(indexFile.toFile()))) {
            return (Map<String, Long>) indexObjIn.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to load index", e);
        } catch (EOFException | FileNotFoundException e) {
            log.info("Index file is empty");
            return new TreeMap<>();
        }
    }
}
