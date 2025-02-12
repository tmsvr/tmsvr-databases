package com.tmsvr.databases.lsmtree.sstable.index;

import static com.tmsvr.databases.lsmtree.sstable.LsmSerDe.SEPARATOR;
import static org.junit.jupiter.api.Assertions.*;

import com.tmsvr.databases.lsmtree.TestUtils;
import com.tmsvr.databases.serde.SerDe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.*;

class IndexTest {

    private static final String FILENAME = "testIndexFile";
    private static final Long OFFSET = 123L;
    private static final String KEY = "key1";

    private SerDe<String> keySerDe;
    private Index<String> index;

    @BeforeEach
    void setUp() throws IOException {
        keySerDe = TestUtils.stringSerDe();
        index = new Index<>(FILENAME, keySerDe);
    }

    @AfterEach
    void cleanup() throws IOException {
        Files.deleteIfExists(Path.of(FILENAME + ".index"));
    }

    @Test
    void testConstructorLoadsIndexFile() throws IOException {
        index.add(KEY, OFFSET);
        index.saveToDisk();

        Index<String> newIndex = new Index<>(FILENAME, keySerDe);

        assertEquals(1, newIndex.getSize());
        assertEquals(OFFSET, newIndex.getOffset(KEY));
    }

    @Test
    void testAdd() {
        index.add(KEY, OFFSET);

        assertEquals(1, index.getSize());
        assertEquals(OFFSET, index.getOffset(KEY));
    }

    @Test
    void testExistsWhenFileDoesNotExist() {
        assertFalse(index.exists());
    }

    @Test
    void testExistsWhenFileExists() throws IOException {
        index.add(KEY, OFFSET);
        index.saveToDisk();

        assertTrue(index.exists());
    }

    @Test
    void testSaveToDisk() throws IOException {
        index.add(KEY, OFFSET);
        index.saveToDisk();

        Path indexFile = Path.of(FILENAME + ".index");
        assertTrue(Files.exists(indexFile));

        String content = Files.readString(indexFile);
        assertTrue(content.contains(KEY + SEPARATOR + OFFSET));
    }

    @Test
    void testGetSize() {
        index.add(KEY, OFFSET);
        assertEquals(1, index.getSize());
    }

    @Test
    void testGetOffsetWhenKeyExists() {
        index.add(KEY, OFFSET);
        assertEquals(OFFSET, index.getOffset(KEY));
    }

    @Test
    void testGetOffsetWhenKeyDoesNotExist() {
        assertNull(index.getOffset("nonExistentKey"));
    }

    @Test
    void testLoadFromDisk() throws IOException {
        index.add(KEY, OFFSET);
        index.saveToDisk();

        Index<String> loadedIndex = new Index<>(FILENAME, keySerDe);
        assertEquals(1, loadedIndex.getSize());
        assertEquals(OFFSET, loadedIndex.getOffset(KEY));
    }

    @Test
    void testAddMultipleEntries() throws IOException {
        index.add("key1", 100L);
        index.add("key2", 200L);
        index.add("key3", 300L);
        index.saveToDisk();

        Index<String> loadedIndex = new Index<>(FILENAME, keySerDe);
        assertEquals(3, loadedIndex.getSize());
        assertEquals(100L, loadedIndex.getOffset("key1"));
        assertEquals(200L, loadedIndex.getOffset("key2"));
        assertEquals(300L, loadedIndex.getOffset("key3"));
    }

}
