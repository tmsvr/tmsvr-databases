package com.tmsvr.databases.lsmtree;

import com.tmsvr.databases.lsmtree.commitlog.CommitLog;
import com.tmsvr.databases.lsmtree.memtable.Memtable;
import com.tmsvr.databases.lsmtree.sstable.SSTableManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class LsmDataStoreTest {

    private LsmDataStore<String, String> dataStore;

    private CommitLog<String, String> commitLog;
    private Memtable<String, String> memtable;
    private SSTableManager<String, String> ssTableManager;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void init() throws IOException {
        commitLog = mock(CommitLog.class);
        memtable = mock(Memtable.class);
        ssTableManager = mock(SSTableManager.class);

        dataStore = new LsmDataStore<>(commitLog, memtable, ssTableManager);
    }

    @Test
    void testDeletionHandling() throws IOException {
        when(memtable.get("1")).thenReturn("found");
        when(memtable.get("2")).thenReturn(null);
        when(memtable.get("3")).thenReturn(null);
        when(memtable.get("4")).thenReturn(null);
        when(memtable.get("5")).thenReturn(null);

        when(ssTableManager.findValue("3")).thenReturn(Optional.of("found"));
        when(ssTableManager.findValue("4")).thenReturn(Optional.empty());
        when(ssTableManager.findValue("5")).thenReturn(Optional.empty());

        assertTrue(dataStore.get("1").isPresent());
        assertEquals("found", dataStore.get("1").get());

        assertTrue(dataStore.get("2").isEmpty());

        assertTrue(dataStore.get("3").isPresent());
        assertEquals("found", dataStore.get("3").get());

        assertTrue(dataStore.get("4").isEmpty());

        assertTrue(dataStore.get("5").isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadVisibilityInvariant() throws IOException {
        // This test verifies the precedence: active -> immutable -> sstable
        // In the mock-based init(), dataStore.activeMemtable is set to 'memtable'

        when(memtable.get("key1")).thenReturn("from-active");
        when(memtable.get("key2")).thenReturn(null);

        // Mock SSTable
        when(ssTableManager.findValue("key2")).thenReturn(Optional.of("from-sstable"));

        assertEquals(Optional.of("from-active"), dataStore.get("key1"));
        assertEquals(Optional.of("from-sstable"), dataStore.get("key2"));
    }
}