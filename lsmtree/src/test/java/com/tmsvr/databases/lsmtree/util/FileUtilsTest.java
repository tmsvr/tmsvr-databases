package com.tmsvr.databases.lsmtree.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.*;
import java.util.function.Function;

class FileUtilsTest {

    private static final Path TEST_FILE = Path.of("testFile.txt");
    private static final String TEST_CONTENT = "Test content";
    private static final String DEFAULT_CONTENT = "Default content";

    @AfterEach
    void cleanup() throws IOException {
        Files.deleteIfExists(TEST_FILE);
    }

    @Test
    void testSaveToDisk() throws IOException {
        FileUtils.saveToDisk(TEST_FILE, TEST_CONTENT, Function.identity());

        assertTrue(Files.exists(TEST_FILE));

        String content = Files.readString(TEST_FILE);
        assertEquals(TEST_CONTENT, content);
    }

    @Test
    void testSaveToDiskWithIOException() {
        Path invalidPath = Paths.get("nonexistent/directory/testFile.txt");

        assertThrows(IOException.class, () -> FileUtils.saveToDisk(invalidPath, TEST_CONTENT, Function.identity()));
    }

    @Test
    void testLoadFromDisk() throws IOException {
        FileUtils.saveToDisk(TEST_FILE, TEST_CONTENT, Function.identity());

        String content = FileUtils.loadFromDisk(TEST_FILE, Function.identity(), DEFAULT_CONTENT);

        assertEquals(TEST_CONTENT, content);
    }

    @Test
    void testLoadFromDiskWithNonExistentFile() throws IOException {
        String content = FileUtils.loadFromDisk(TEST_FILE, Function.identity(), DEFAULT_CONTENT);

        assertEquals(DEFAULT_CONTENT, content);
    }

    @Test
    void testSaveAndLoadWithComplexObject() throws IOException {
        Person person = new Person("John", 30);

        FileUtils.saveToDisk(TEST_FILE, person, p -> p.name + "," + p.age);

        Person loadedPerson = FileUtils.loadFromDisk(TEST_FILE, data -> {
            String[] parts = data.split(",");
            return new Person(parts[0], Integer.parseInt(parts[1]));
        }, new Person("Default", 0));

        assertEquals("John", loadedPerson.name);
        assertEquals(30, loadedPerson.age);
    }

    record Person(String name, int age) { }
}
