package com.tmsvr.databases.lsmtree;

import com.tmsvr.databases.DataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.tmsvr.databases.lsmtree.TestUtils.stringSerDe;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

public class DataStorePerformanceTest {
    private static final int ITEM_COUNT = 400_000;
    private static final int MEMTABLE_SIZE = 50_000;

    private DataStore<String, String> dataStore;

    @BeforeEach
    public void setUp() throws IOException {
        try {
            cleanup();
        } catch (IOException e) {
            System.out.println("No files to clean up");
        }
        dataStore = new LsmDataStore<>(stringSerDe(), stringSerDe(), MEMTABLE_SIZE);
    }

    @Test
    public void testPutPerformance() throws IOException {
        long durationSum = 0;

        for (int i = 0; i < 5; i++) {
            dataStore = new LsmDataStore<>(stringSerDe(), stringSerDe(), MEMTABLE_SIZE);

            long startTime = System.nanoTime();
            createData();
            long duration = System.nanoTime() - startTime;
            durationSum += duration;

            System.out.println("Time to put " + ITEM_COUNT + " items: " + duration / 1_000_000_000.0 + " s");

            cleanup();
        }

        System.out.println("Average time to put " + ITEM_COUNT + " items: " + (durationSum / 5) / 1_000_000_000.0 + " s");
    }

    @Test
    public void testGetPerformance() throws IOException {int numRandomGets = 100_000;
        long durationSum = 0;

        createData();

        for (int i = 0; i < 5; i++) {
            Random random = new Random();
            Set<String> keysToGet = new HashSet<>();
            while (keysToGet.size() < numRandomGets) {
                int randomIndex = random.nextInt(ITEM_COUNT);
                keysToGet.add("key" + randomIndex);
            }

            long startTime = System.nanoTime();

            for (String key : keysToGet) {
                Optional<String> value = dataStore.get(key);
                assertTrue(value.isPresent(), "Value for " + key + " should be present");
            }

            long duration = System.nanoTime() - startTime;
            durationSum += duration;
            System.out.println("Time to get " + numRandomGets + " random items: " + duration / 1_000_000_000.0 + " s");
        }

        System.out.println("Average time to get " + numRandomGets + " random items: " + (durationSum / 5) / 1_000_000_000.0 + " s");
    }

    private static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuilder stringBuilder = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int randomIndex = random.nextInt(characters.length());
            stringBuilder.append(characters.charAt(randomIndex));
        }

        return stringBuilder.toString();
    }

    private void createData() throws IOException {
        for (int j = 0; j < ITEM_COUNT; j++) {
            dataStore.put("key" + j, "value-" + j + "-" + generateRandomString(500));
        }
    }

    private void cleanup() throws IOException {
        Path dir = Path.of(".");

        try (Stream<Path> paths = Files.walk(dir)) {
            paths.filter(path -> path.toString().endsWith(".index") || path.toString().endsWith(".filter") || path.toString().endsWith(".data"))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }

        Files.delete(Path.of("./commit-log.txt"));
    }
}

