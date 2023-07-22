package com.tmsvr.databases.lsmtree.memtable;

import com.tmsvr.databases.DataRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MemtableTest {

    @Test
    void testInitialisationOk() {
        Memtable memtable = new Memtable();
        assertEquals(0, memtable.getSize());
    }

    @Test
    void testInitialisationWithDataOk() {
        List<DataRecord> recordList = List.of(
                new DataRecord("a", "b"),
                new DataRecord("a", "c"),
                new DataRecord("b", "d")
        );

        Memtable memtable = new Memtable(recordList);

        assertEquals(2, memtable.getSize());
        assertEquals("c", memtable.get("a"));
        assertEquals("d", memtable.get("b"));
    }

    @Test
    void testPutOk() {
        Memtable memtable = new Memtable();
        memtable.put(new DataRecord("a", "b"));
        memtable.put(new DataRecord("a", "c"));
        memtable.put(new DataRecord("b", "d"));

        assertEquals(2, memtable.getSize());
        assertEquals("c", memtable.get("a"));
        assertEquals("d", memtable.get("b"));
    }

    @Test
    void testClearOk() {
        Memtable memtable = new Memtable();
        memtable.put(new DataRecord("a", "b"));
        memtable.put(new DataRecord("a", "c"));
        memtable.put(new DataRecord("b", "d"));

        memtable.clear();

        assertEquals(0, memtable.getSize());
        assertNull(memtable.get("a"));
        assertNull(memtable.get("b"));
    }

}