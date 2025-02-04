package com.tmsvr.databases.lsmtree;

import com.tmsvr.databases.lsmtree.memtable.Memtable;
import com.tmsvr.databases.lsmtree.sstable.SSTableManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LsmDataStoreTest {

    private LsmDataStore<String, String> dataStore;

    private Memtable<String, String> memtable;
    private SSTableManager<String, String> ssTableManager;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void init() {
        memtable = mock(Memtable.class);
        ssTableManager = mock(SSTableManager.class);

        dataStore = new LsmDataStore<>(null, memtable, ssTableManager);
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

}