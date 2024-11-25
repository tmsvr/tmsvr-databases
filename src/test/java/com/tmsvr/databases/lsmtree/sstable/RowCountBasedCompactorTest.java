package com.tmsvr.databases.lsmtree.sstable;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.lsmtree.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RowCountBasedCompactorTest {

    @BeforeEach
    @AfterEach
    void cleanup() throws IOException {
        TestUtils.cleanupFiles();
    }

    @Test
    void testMergeIsOk() throws IOException {
        RowCountBasedCompactor compactor = new RowCountBasedCompactor(3);

        SSTable older = new SSTable("table-1");
        SSTable newer = new SSTable("table-2");

        older.write(Map.of("k2", "v2", "k3", "v3", "k4", "v4", "k6", "v6"));
        newer.write(Map.of("k1", "v1", "k4", "v4-2", "k5", "v5", "k7", "v7", "k8", "v8"));

        SSTable result = compactor.merge(older, newer);

        assertNotNull(result);

        List<DataRecord> records = result.getAllLines();

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
        SSTable table1 = new SSTable("table-1");
        SSTable table2 = new SSTable("table-2");
        SSTable table3 = new SSTable("table-3");
        SSTable table4 = new SSTable("table-4");
        SSTable table5 = new SSTable("table-5");
        SSTable table6 = new SSTable("table-6");
        SSTable table7 = new SSTable("table-7");

        table1.write(Map.of("k2", "v2", "k3", "v3", "k4", "v4", "k6", "v6"));
        table2.write(Map.of("k1", "v1"));
        table3.write(Map.of("k4", "v4-2"));
        table4.write(Map.of("k5", "v5", "k6", "v6-2"));
        table5.write(Map.of("k7", "v7", "k8", "v8"));
        table6.write(Map.of("k9", "v9", "k99", "v99"));
        table7.write(Map.of("k999", "v999"));

        RowCountBasedCompactor compactor = new RowCountBasedCompactor(3);

        List<SSTable> result = compactor.compact(List.of(table1, table2, table3, table4, table5, table6, table7));

        assertNotNull(result);
        assertEquals(4, result.size());
    }

}