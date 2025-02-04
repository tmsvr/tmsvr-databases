package com.tmsvr.databases.lsmtree.sstable;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.lsmtree.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.tmsvr.databases.lsmtree.TestUtils.stringSerDe;
import static org.junit.jupiter.api.Assertions.*;

class RowCountBasedCompactorTest {

    @BeforeEach
    @AfterEach
    void cleanup() throws IOException {
        TestUtils.cleanupFiles();
    }

    @Test
    void testMergeIsOk() throws IOException {
        RowCountBasedCompactor<String, String> compactor = new RowCountBasedCompactor<>(3);

        SSTable<String, String> older = new SSTable<>("table-1", stringSerDe(), stringSerDe());
        SSTable<String, String> newer = new SSTable<>("table-2", stringSerDe(), stringSerDe());

        older.write(Map.of("k2", "v2", "k3", "v3", "k4", "v4", "k6", "v6"));
        newer.write(Map.of("k1", "v1", "k4", "v4-2", "k5", "v5", "k7", "v7", "k8", "v8"));

        SSTable<String, String> result = compactor.merge(older, newer);

        assertNotNull(result);

        List<DataRecord<String, String>> records = result.getAllLines();

        assertEquals(8, records.size());

        for (int i = 0; i < records.size() - 1; i++) {
            assertTrue(records.get(i).key().compareTo(records.get(i + 1).key()) < 0);

            if (records.get(i).key().equals("k4")) {
                assertEquals("v4-2", records.get(i).value());
            }
        }
    }

    @Test
    void testCompactionIsOk() throws IOException {
        SSTable<String, String> table1 = new SSTable<>("table-1", stringSerDe(), stringSerDe());
        SSTable<String, String> table2 = new SSTable<>("table-2", stringSerDe(), stringSerDe());
        SSTable<String, String> table3 = new SSTable<>("table-3", stringSerDe(), stringSerDe());
        SSTable<String, String> table4 = new SSTable<>("table-4", stringSerDe(), stringSerDe());
        SSTable<String, String> table5 = new SSTable<>("table-5", stringSerDe(), stringSerDe());
        SSTable<String, String> table6 = new SSTable<>("table-6", stringSerDe(), stringSerDe());
        SSTable<String, String> table7 = new SSTable<>("table-7", stringSerDe(), stringSerDe());

        table1.write(Map.of("k2", "v2", "k3", "v3", "k4", "v4", "k6", "v6"));
        table2.write(Map.of("k1", "v1"));
        table3.write(Map.of("k4", "v4-2"));
        table4.write(Map.of("k5", "v5", "k6", "v6-2"));
        table5.write(Map.of("k7", "v7", "k8", "v8"));
        table6.write(Map.of("k9", "v9", "k99", "v99"));
        table7.write(Map.of("k999", "v999"));

        RowCountBasedCompactor<String, String> compactor = new RowCountBasedCompactor<>(3);

        List<SSTable<String, String>> result = compactor.compact(List.of(table1, table2, table3, table4, table5, table6, table7));

        assertNotNull(result);
        assertEquals(4, result.size());
    }

}