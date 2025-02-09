package com.tmsvr.databases.lsmtree.commitlog;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.lsmtree.sstable.LsmSerDe;
import com.tmsvr.databases.serde.SerDe;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

import static com.tmsvr.databases.lsmtree.sstable.LsmSerDe.SEPARATOR;

@Slf4j
public class DefaultCommitLog<K extends Comparable<K>,V> implements CommitLog<K,V> {

    static final String FILE_PATH = "commit-log.txt";
    private final SerDe<K> keySerDe;
    private final SerDe<V> valueSerDe;
    private long size;


    public DefaultCommitLog(LsmSerDe<K> keySerDe, LsmSerDe<V> valueSerDe) throws IOException {
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
        this.size = 0;

        if (Files.exists(Paths.get(FILE_PATH))) {
            log.info("Commit Log already exists");
            size = countLinesInLog();
        } else {
            createFile();
        }
    }

    private void createFile() throws IOException {
        Files.createFile(Paths.get(FILE_PATH));
    }

    private long countLinesInLog() throws IOException {
        try (Stream<String> lines = Files.lines(Paths.get(DefaultCommitLog.FILE_PATH))) {
            return lines.count();
        }
    }

    private String entryToString(DataRecord<K,V> entry) {
        return keySerDe.serialize(entry.key()) + SEPARATOR + valueSerDe.serialize(entry.value()) + System.lineSeparator();
    }

    @Override
    public void append(DataRecord<K,V> entry) throws IOException {
        Files.write(Paths.get(FILE_PATH), entryToString(entry).getBytes(), StandardOpenOption.APPEND);
        size++;
    }

    @Override
    public List<DataRecord<K,V>> readCommitLog() throws IOException {
        return Files.readAllLines(Paths.get(FILE_PATH))
                .stream()
                .map(line -> {
                    String[] split = line.split(SEPARATOR);
                    return new DataRecord<>(keySerDe.deserialize(split[0]), valueSerDe.deserialize(split[1]));
                }).toList();
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public void clear() throws IOException {
        Files.deleteIfExists(Paths.get(FILE_PATH));
        createFile();
        size = 0;
    }
}
