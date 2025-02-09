package com.tmsvr.databases.lsmtree;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.DataStore;
import com.tmsvr.databases.lsmtree.commitlog.CommitLog;
import com.tmsvr.databases.lsmtree.commitlog.DefaultCommitLog;
import com.tmsvr.databases.lsmtree.memtable.Memtable;
import com.tmsvr.databases.lsmtree.sstable.LsmSerDe;
import com.tmsvr.databases.lsmtree.sstable.SSTableManager;
import com.tmsvr.databases.serde.SerDe;

import java.io.IOException;
import java.util.Optional;

public class LsmDataStore<K extends Comparable<K>, V> implements DataStore<K, V> {
    private static final long FLUSH_TO_DISK_LIMIT = 5;

    private final CommitLog<K,V> commitLog;
    private final Memtable<K, V> memtable;
    private final SSTableManager<K, V> ssTableManager;

    public LsmDataStore(LsmSerDe<K> keySerDe, LsmSerDe<V> valueSerDe) throws IOException {
        this.commitLog = new DefaultCommitLog<>(keySerDe, valueSerDe);

        if (this.commitLog.getSize() > 0) {
            this.memtable = new Memtable<>(this.commitLog.readCommitLog());
        } else {
            this.memtable = new Memtable<>();
        }

        ssTableManager = new SSTableManager<>(keySerDe, valueSerDe);
        ssTableManager.readTablesFromFile();
    }

    LsmDataStore(CommitLog<K,V> commitLog, Memtable<K, V> memtable, SSTableManager<K, V> ssTableManager) {
        this.commitLog = commitLog;
        this.memtable = memtable;
        this.ssTableManager = ssTableManager;
    }

    @Override
    public void put(K key, V value) throws IOException {
        DataRecord<K, V> dataRecord = new DataRecord<>(key, value);
        commitLog.append(dataRecord);
        memtable.put(dataRecord);

        if (memtable.getSize() > FLUSH_TO_DISK_LIMIT) {
            flush();
        }
    }

    @Override
    public Optional<V> get(K key) {
        return Optional.ofNullable(memtable.get(key))
                .or(() -> {
                    try {
                        return ssTableManager.findValue(key);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void delete(K key) throws IOException {
        put(key, null);
    }

    public void flush() throws IOException {
        ssTableManager.flush(memtable.getAsMap());
        memtable.clear();
        commitLog.clear();
    }
}
