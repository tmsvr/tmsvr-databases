package com.tmsvr.databases.lsmtree.commitlog;

import com.tmsvr.databases.lsmtree.sstable.LsmSerDe;
import com.tmsvr.databases.serde.SerDe;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Factory for creating and recovering commit log segments.
 * This replaces the old single-file commit log with a segment-per-memtable
 * model.
 */
@Slf4j
public class DefaultCommitLog<K extends Comparable<K>, V> implements CommitLog<K, V> {
    private final SerDe<K> keySerDe;
    private final SerDe<V> valueSerDe;

    public DefaultCommitLog(LsmSerDe<K> keySerDe, LsmSerDe<V> valueSerDe) {
        this.keySerDe = keySerDe;
        this.valueSerDe = valueSerDe;
    }

    @Override
    public CommitLogSegment<K, V> createSegment() throws IOException {
        String segmentId = UUID.randomUUID().toString();
        log.debug("Creating new commit log segment: {}", segmentId);
        return new CommitLogSegment<>(segmentId, keySerDe, valueSerDe);
    }

    @Override
    public List<CommitLogSegment<K, V>> recoverSegments() throws IOException {
        List<CommitLogSegment<K, V>> segments = new ArrayList<>();
        Path rootPath = Paths.get("");

        try (Stream<Path> paths = Files.find(rootPath, 1,
                (path, attrs) -> path.toString().matches(".*commit-log-.*\\.wal$"))) {
            paths.forEach(path -> {
                try {
                    // Extract segment ID from filename: commit-log-{UUID}.wal
                    String filename = path.getFileName().toString();
                    String segmentId = filename.replace("commit-log-", "").replace(".wal", "");

                    log.info("Recovering commit log segment: {}", segmentId);
                    CommitLogSegment<K, V> segment = new CommitLogSegment<>(segmentId, keySerDe, valueSerDe);
                    segments.add(segment);
                } catch (IOException e) {
                    log.error("Failed to recover segment: {}", path, e);
                }
            });
        }

        log.info("Recovered {} commit log segments", segments.size());
        return segments;
    }
}
