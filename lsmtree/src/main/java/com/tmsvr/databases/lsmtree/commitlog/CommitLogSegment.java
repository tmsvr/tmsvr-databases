package com.tmsvr.databases.lsmtree.commitlog;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.serde.SerDe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static com.tmsvr.databases.lsmtree.sstable.LsmSerDe.SEPARATOR;

/**
 * A commit log segment that is owned by a single Memtable.
 * Each segment corresponds to one memtable instance for atomic rotation.
 * Thread-safe: append() is synchronized to prevent FileChannel interleaving.
 */
@Slf4j
public class CommitLogSegment<K extends Comparable<K>, V> {
    @Getter
    private final String segmentId;
    private final SerDe<K> keySerDe;
    private final SerDe<V> valueSerDe;
    private final FileChannel fileChannel;
    private final Path filePath;

    public CommitLogSegment(String segmentId, SerDe<K> keySerDe, SerDe<V> valueSerDe) throws IOException {
        this.segmentId = segmentId;
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
        this.filePath = Paths.get("commit-log-" + segmentId + ".txt");

        this.fileChannel = FileChannel.open(
                filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND,
                StandardOpenOption.DSYNC);

        log.debug("Created commit log segment: {}", segmentId);
    }

    /**
     * Append a record to this segment.
     * Synchronized to prevent concurrent FileChannel writes from corrupting the WAL.
     */
    public synchronized void append(DataRecord<K, V> record) throws IOException {
        String line = keySerDe.serialize(record.key()) + SEPARATOR + valueSerDe.serialize(record.value()) + System.lineSeparator();
        ByteBuffer buffer = ByteBuffer.wrap(line.getBytes());
        fileChannel.write(buffer);
    }

    /**
     * Close the file channel for this segment.
     */
    public void close() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.force(true); // Ensure all data is flushed
            fileChannel.close();
            log.debug("Closed commit log segment: {}", segmentId);
        }
    }

    /**
     * Delete this segment file after successful flush to SSTable.
     */
    public void delete() throws IOException {
        close();
        if (Files.deleteIfExists(filePath)) {
            log.debug("Deleted commit log segment: {}", segmentId);
        }
    }
}
