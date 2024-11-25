package com.tmsvr.databases.lsmtree.sstable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.FILENAME;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.KEY_1;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.KEY_2;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.KEY_3;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.VALUE_1;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.VALUE_2;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.VALUE_3;
import static com.tmsvr.databases.lsmtree.sstable.SSTableFixtures.aDataSet;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SSTableTest {

    private SSTable ssTable;

    @BeforeEach
    void setup() throws IOException {
        ssTable = new SSTable(FILENAME);
    }

    @AfterEach
    void cleanup() throws IOException {
        Files.deleteIfExists(Path.of(FILENAME + ".index"));
        Files.deleteIfExists(Path.of(FILENAME + ".data"));
    }

    @Test
    void testWriteAndRead() throws IOException {
        Map<String, String> data = aDataSet();
        ssTable.write(data);

        assertTrue(Files.exists(Path.of(FILENAME + ".index")));
        assertTrue(Files.exists(Path.of(FILENAME + ".data")));

        assertTrue(ssTable.getValue(KEY_1).isPresent());
        assertTrue(ssTable.getValue(KEY_2).isPresent());
        assertTrue(ssTable.getValue(KEY_3).isPresent());

        assertEquals(VALUE_1, ssTable.getValue(KEY_1).get());
        assertEquals(VALUE_2, ssTable.getValue(KEY_2).get());
        assertEquals(VALUE_3, ssTable.getValue(KEY_3).get());
    }

    @Test
    void testGetValueNotFound() throws IOException {
        Map<String, String> data = new HashMap<>();
        data.put(KEY_1, VALUE_1);
        ssTable.write(data);

        assertTrue(ssTable.getValue("invalid-key").isEmpty());
    }
}