package com.tmsvr.sstable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.tmsvr.sstable.SSTableFixtures.KEY_1;
import static com.tmsvr.sstable.SSTableFixtures.KEY_2;
import static com.tmsvr.sstable.SSTableFixtures.KEY_3;
import static com.tmsvr.sstable.SSTableFixtures.VALUE_1;
import static com.tmsvr.sstable.SSTableFixtures.VALUE_2;
import static com.tmsvr.sstable.SSTableFixtures.VALUE_3;
import static com.tmsvr.sstable.SSTableFixtures.aDataSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SSTableManagerTest {

    private SSTableManager manager;

    @BeforeEach
    void setup() throws IOException {
        cleanupFiles();
        manager = new SSTableManager();
    }

    @AfterEach
    void cleanup() throws IOException {
        cleanupFiles();
    }

    private void cleanupFiles() throws IOException {
        Files.deleteIfExists(Path.of("sstable-0.index"));
        Files.deleteIfExists(Path.of("sstable-0.data"));
        Files.deleteIfExists(Path.of("sstable-1.index"));
        Files.deleteIfExists(Path.of("sstable-1.data"));
        Files.deleteIfExists(Path.of("sstable-2.index"));
        Files.deleteIfExists(Path.of("sstable-2.data"));
    }

    @Test
    void testFlushOk() throws IOException {
        Map<String, String> data = aDataSet();

        manager.flush(data);

        assertTrue(Files.exists(Path.of( "sstable-0.index")));
        assertTrue(Files.exists(Path.of("sstable-0.data")));

        for (Map.Entry<String, String> entry : data.entrySet()) {
            assertEquals(entry.getValue(), manager.findValue(entry.getKey()));
        }
    }

    @Test
    void testFlushMultipleTablesOk() throws IOException {
        Map<String, String> data0 = Map.of(KEY_1, VALUE_1);
        Map<String, String> data1 = Map.of(KEY_2, VALUE_2);
        Map<String, String> data2 = Map.of(KEY_3, VALUE_3);

        manager.flush(data0);
        manager.flush(data1);
        manager.flush(data2);

        assertTrue(Files.exists(Path.of( "sstable-0.index")));
        assertTrue(Files.exists(Path.of("sstable-0.data")));
        assertTrue(Files.exists(Path.of( "sstable-1.index")));
        assertTrue(Files.exists(Path.of("sstable-1.data")));
        assertTrue(Files.exists(Path.of( "sstable-2.index")));
        assertTrue(Files.exists(Path.of("sstable-2.data")));

        assertEquals(VALUE_1, manager.findValue(KEY_1));
        assertEquals(VALUE_2, manager.findValue(KEY_2));
        assertEquals(VALUE_3, manager.findValue(KEY_3));
    }

    @Test
    void testFindValueMultipleSSTablesSingleOccurrence() throws IOException {
        Map<String, String> data = aDataSet();

        manager.flush(data);
        manager.flush(Map.of("newKey", "newValue", "anotherNewKey", "moreNewValues"));

        for (Map.Entry<String, String> entry : data.entrySet()) {
            assertEquals(entry.getValue(), manager.findValue(entry.getKey()));
        }

        assertEquals("newValue", manager.findValue("newKey"));
        assertEquals("moreNewValues", manager.findValue("anotherNewKey"));
    }

    @Test
    void testFindValueMultipleSSTablesMultipleOccurrences() throws IOException {
        Map<String, String> data = aDataSet();

        manager.flush(data);
        manager.flush(Map.of("newKey", "newValue", KEY_1, "moreNewValues"));

        assertEquals("newValue", manager.findValue("newKey"));
        assertEquals("moreNewValues", manager.findValue(KEY_1));
        assertEquals(VALUE_2, manager.findValue(KEY_2));
        assertEquals(VALUE_3, manager.findValue(KEY_3));
    }

    @Test
    void testReadTablesFromFiles() throws IOException {
        Map<String, String> data0 = Map.of(KEY_1, VALUE_1);
        Map<String, String> data1 = Map.of(KEY_2, VALUE_2);
        Map<String, String> data2 = Map.of(KEY_3, VALUE_3, "k4", "v4");

        manager.flush(data0);
        manager.flush(data1);
        manager.flush(data2);

        SSTableManager newManager = new SSTableManager();
        newManager.readTablesFromFile();

        assertEquals(VALUE_1, newManager.findValue(KEY_1));
        assertEquals(VALUE_2, newManager.findValue(KEY_2));
        assertEquals(VALUE_3, newManager.findValue(KEY_3));
        assertEquals("v4", newManager.findValue("k4"));
    }
}