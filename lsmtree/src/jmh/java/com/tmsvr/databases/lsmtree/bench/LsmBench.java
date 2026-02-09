package com.tmsvr.databases.lsmtree.bench;

import com.tmsvr.databases.DataStore;
import com.tmsvr.databases.lsmtree.LsmDataStore;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.tmsvr.databases.lsmtree.TestUtils.stringSerDe;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
public class LsmBench {

    // ---------------- configuration ----------------

    static final int ITEM_COUNT = 400_000;
    static final int MEMTABLE_SIZE = 100_000;

    // ---------------- benchmark state ----------------

    @State(Scope.Benchmark)
    public static class LsmState {

        DataStore<String, String> dataStore;
        List<String> keys;
        List<String> values;
        AtomicInteger writeCounter;
        AtomicInteger readCounter;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            cleanup();

            dataStore = new LsmDataStore<>(
                    stringSerDe(),
                    stringSerDe(),
                    MEMTABLE_SIZE
            );

            // Pre-generate values (no allocation during benchmark)
            keys = IntStream.range(0, ITEM_COUNT)
                    .mapToObj(i -> "key" + i)
                    .collect(Collectors.toCollection(ArrayList::new));

            values = IntStream.range(0, ITEM_COUNT)
                    .mapToObj(i -> "value-" + i + "-" + randomString(500))
                    .collect(Collectors.toCollection(ArrayList::new));

            writeCounter = new AtomicInteger();
            readCounter = new AtomicInteger();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            cleanup();
        }
    }

    // ---------------- PUT benchmarks ----------------

    @Benchmark
    @Threads(1)
    public void singleThreadPut(LsmState state) throws IOException {
        int i = state.writeCounter.getAndIncrement();
        int idx = Math.floorMod(i, ITEM_COUNT);

        state.dataStore.put(state.keys.get(idx), state.values.get(idx));
    }

    @Benchmark
    @Threads(8)
    public void multiThreadPut(LsmState state) throws IOException {
        int i = state.writeCounter.getAndIncrement();
        int idx = Math.floorMod(i, ITEM_COUNT);

        state.dataStore.put(state.keys.get(idx), state.values.get(idx));
    }

    // ---------------- GET benchmarks ----------------

    @Benchmark
    @Threads(1)
    public Optional<String> hotRead(LsmState state) throws Exception {
        return state.dataStore.get(state.keys.getFirst());
    }

    @Benchmark
    @Threads(1)
    public Optional<String> sequentialRead(LsmState state) throws IOException {
        int i = state.readCounter.getAndIncrement();
        int idx = Math.floorMod(i, ITEM_COUNT);

        return state.dataStore.get(state.keys.get(idx));
    }

    @Benchmark
    @Threads(8)
    public Optional<String> concurrentRead(LsmState state) throws Exception {
        int i = state.readCounter.getAndIncrement();
        int idx = Math.floorMod(i, ITEM_COUNT);

        return state.dataStore.get(state.keys.get(idx));
    }

    // ---------------- mixed workload ----------------

    @Benchmark
    @Threads(8)
    public void mixedReadWrite(LsmState state) throws Exception {
        int c = state.writeCounter.get();
        if ((c & 7) == 0) { // ~12.5% reads
            int i = state.readCounter.getAndIncrement();
            int idx = Math.floorMod(i, ITEM_COUNT);

            state.dataStore.get(state.keys.get(idx));
        } else {
            int i = state.writeCounter.getAndIncrement();
            int idx = Math.floorMod(i, ITEM_COUNT);

            state.dataStore.put(state.keys.get(idx), state.values.get(idx)
            );
        }
    }

    // ---------------- helpers ----------------

    private static String randomString(int length) {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = (char) ('a' + (i % 26));
        }
        return new String(chars);
    }

    private static void cleanup() throws IOException {
        Path dir = Path.of(".");

        try (var paths = Files.walk(dir)) {
            paths.filter(p ->
                    p.toString().endsWith(".index")
                            || p.toString().endsWith(".filter")
                            || p.toString().endsWith(".data")
                            || p.toString().endsWith("commit-log.txt")
            ).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
            });
        }
    }
}
