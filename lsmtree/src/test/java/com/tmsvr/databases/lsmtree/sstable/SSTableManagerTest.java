package com.tmsvr.databases.lsmtree.sstable;

import com.tmsvr.databases.lsmtree.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static com.tmsvr.databases.lsmtree.TestUtils.stringSerDe;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.KEY_1;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.KEY_2;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.KEY_3;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.VALUE_1;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.VALUE_2;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.VALUE_3;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.aDataSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SSTableManagerTest {

    private SSTableManager<String, String> manager;

    @BeforeEach
    void setup() throws IOException {
        TestUtils.cleanupFiles();
        manager = new SSTableManager<>(stringSerDe(), stringSerDe());
    }

    @AfterEach
    void cleanup() throws IOException {
        TestUtils.cleanupFiles();
    }

    @Test
    void testFlushOk() throws IOException {
        Map<String, String> data = aDataSet();

        manager.flush(data);

        assertSSTablesFlushed(1);

        for (Map.Entry<String, String> entry : data.entrySet()) {
            if (entry.getValue() != null) {
                assertTrue(manager.findValue(entry.getKey()).isPresent());
                assertEquals(entry.getValue(), manager.findValue(entry.getKey()).get());
            } else {
                assertTrue(manager.findValue(entry.getKey()).isEmpty());
            }
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

        assertSSTablesFlushed(3);

        assertTrue(manager.findValue(KEY_1).isPresent());
        assertTrue(manager.findValue(KEY_2).isPresent());
        assertTrue(manager.findValue(KEY_3).isPresent());

        assertEquals(VALUE_1, manager.findValue(KEY_1).get());
        assertEquals(VALUE_2, manager.findValue(KEY_2).get());
        assertEquals(VALUE_3, manager.findValue(KEY_3).get());
    }

    @Test
    void testFindValueMultipleSSTablesSingleOccurrence() throws IOException {
        Map<String, String> data = aDataSet();

        manager.flush(data);
        manager.flush(Map.of("newKey", "newValue", "anotherNewKey", "moreNewValues"));

        for (Map.Entry<String, String> entry : data.entrySet()) {
            if (entry.getValue() != null) {
                assertTrue(manager.findValue(entry.getKey()).isPresent());
                assertEquals(entry.getValue(), manager.findValue(entry.getKey()).get());
            } else {
                assertTrue(manager.findValue(entry.getKey()).isEmpty());
            }
        }

        assertTrue(manager.findValue("newKey").isPresent());
        assertTrue(manager.findValue("anotherNewKey").isPresent());

        assertEquals("newValue", manager.findValue("newKey").get());
        assertEquals("moreNewValues", manager.findValue("anotherNewKey").get());
    }

    @Test
    void testFindValueMultipleSSTablesMultipleOccurrences() throws IOException {
        Map<String, String> data = aDataSet();

        manager.flush(data);
        manager.flush(Map.of("newKey", "newValue", KEY_1, "moreNewValues"));

        assertTrue(manager.findValue(KEY_1).isPresent());
        assertTrue(manager.findValue(KEY_2).isPresent());
        assertTrue(manager.findValue(KEY_3).isPresent());
        assertTrue(manager.findValue("newKey").isPresent());

        assertEquals("newValue", manager.findValue("newKey").get());
        assertEquals("moreNewValues", manager.findValue(KEY_1).get());
        assertEquals(VALUE_2, manager.findValue(KEY_2).get());
        assertEquals(VALUE_3, manager.findValue(KEY_3).get());
    }

    @Test
    void testReadTablesFromFiles() throws IOException {
        Map<String, String> data0 = Map.of(KEY_1, VALUE_1);
        Map<String, String> data1 = Map.of(KEY_2, VALUE_2);
        Map<String, String> data2 = Map.of(KEY_3, VALUE_3, "k4", "v4");

        manager.flush(data0);
        manager.flush(data1);
        manager.flush(data2);

        SSTableManager<String, String> newManager = new SSTableManager<>(stringSerDe(), stringSerDe());
        newManager.readTablesFromFile();

        assertTrue(newManager.findValue(KEY_1).isPresent());
        assertTrue(newManager.findValue(KEY_2).isPresent());
        assertTrue(newManager.findValue(KEY_3).isPresent());
        assertTrue(newManager.findValue("k4").isPresent());

        assertEquals(VALUE_1, newManager.findValue(KEY_1).get());
        assertEquals(VALUE_2, newManager.findValue(KEY_2).get());
        assertEquals(VALUE_3, newManager.findValue(KEY_3).get());
        assertEquals("v4", newManager.findValue("k4").get());
    }

    private void assertSSTablesFlushed(int numberOfExpectedTables) throws IOException {
        try(Stream<Path> files = Files.list(Path.of("."))) {
            long filesStartingWithSSTable = files.filter(path -> path.getFileName().toString().startsWith("sstable"))
                    .count();

            assertEquals(numberOfExpectedTables, filesStartingWithSSTable / 2);
        }
    }
}