package com.tmsvr.databases.lsmtree.commitlog;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.lsmtree.sstable.LsmSerDe;
import com.tmsvr.databases.serde.SerDe;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

import static com.tmsvr.databases.lsmtree.sstable.LsmSerDe.SEPARATOR;

@Slf4j
public class DefaultCommitLog<K extends Comparable<K>, V> implements CommitLog<K, V> {
    static final int FLUSH_THRESHOLD = 100;
    static final String FILE_PATH = "commit-log.txt";
    private final SerDe<K> keySerDe;
    private final SerDe<V> valueSerDe;
    private long size;
    private FileChannel fileChannel;
    private int writeCount;

    public DefaultCommitLog(LsmSerDe<K> keySerDe, LsmSerDe<V> valueSerDe) throws IOException {
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
        this.size = 0;
        this.writeCount = 0;

        fileChannel = FileChannel.open(
                Paths.get(FILE_PATH),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND,
                StandardOpenOption.DSYNC
        );

        if (Files.exists(Paths.get(FILE_PATH))) {
            size = countLinesInLog();
        } else {
            createFile();
        }
    }

    private void createFile() throws IOException {
        Files.createFile(Paths.get(FILE_PATH));
    }

    private long countLinesInLog() throws IOException {
        try (Stream<String> lines = Files.lines(Paths.get(FILE_PATH))) {
            return lines.count();
        }
    }

    private String entryToString(DataRecord<K, V> entry) {
        return keySerDe.serialize(entry.key()) + SEPARATOR + valueSerDe.serialize(entry.value()) + System.lineSeparator();
    }

    @Override
    public void append(DataRecord<K, V> entry) throws IOException {
        byte[] data = entryToString(entry).getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        fileChannel.write(buffer);

        writeCount++;
        if (writeCount >= FLUSH_THRESHOLD) {
            fileChannel.force(false);  // Metadata updates not forced (faster)
            writeCount = 0;
        }
        size++;
    }

    @Override
    public List<DataRecord<K, V>> readCommitLog() throws IOException {
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
        fileChannel.force(true);
        fileChannel.close();
        Files.deleteIfExists(Paths.get(FILE_PATH));
        createFile();
        size = 0;
        writeCount = 0;
        fileChannel = FileChannel.open(
                Paths.get(FILE_PATH),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
    }
}
