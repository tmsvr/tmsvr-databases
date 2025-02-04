package com.tmsvr.databases.btree;

import com.tmsvr.databases.DataRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.tmsvr.databases.btree.BTreeNode.ORDER;
import static org.junit.jupiter.api.Assertions.*;

class BTreeStructureTest {
    private BTreeDataStore<String, String> dataStore;

    @BeforeEach
    void setUp() {
        dataStore = new BTreeDataStore<>();
    }

    @Nested
    class TestKnuthDefinition {
        @ParameterizedTest
        @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 35, 50})
        void testEveryNodeHasAtMostMChildren(int numKeys) {
            for (int i = 1; i <= numKeys; i++) {
                dataStore.put(String.format("%d", i), String.format("value%d", i));
            }

            BTreeNode<String, String> rootNode = dataStore.getRoot();

            dataStore.printTree();

            // Every node has at most m children
            traverseAndExecute(rootNode, node -> assertTrue(node.children.size() <= ORDER));
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 35, 50})
        void testEveryNodeExceptForRootAndLeavesHasAtLeastHalfMChildren(int numKeys) {
            for (int i = 1; i <= numKeys; i++) {
                dataStore.put(String.format("%d", i), String.format("value%d", i));
            }

            BTreeNode<String, String> rootNode = dataStore.getRoot();

            dataStore.printTree();

            // Every node has at most m children
            traverseAndExecute(rootNode, node -> {
                if (!node.isLeaf && node != rootNode) {
                    assertTrue(node.children.size() >= ORDER / 2);
                }
            });
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 35, 50})
        void testRootNodeHasAtLeastTwiChildUnlessLeaf(int numKeys) {
            for (int i = 1; i <= numKeys; i++) {
                dataStore.put(String.format("%d", i), String.format("value%d", i));
            }

            BTreeNode<String, String> rootNode = dataStore.getRoot();

            if (!rootNode.isLeaf) {
                assertTrue(rootNode.children.size() >= 2);
            }
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 35, 50})
        void testAllLeavesAreOnTheSameLevel(int numKeys) {
            for (int i = 1; i <= numKeys; i++) {
                dataStore.put(String.format("%d", i), String.format("value%d", i));
            }

            List<Integer> leafDepths = new ArrayList<>();

            checkLeafDepths(dataStore.getRoot(), 0, leafDepths);

            assertEquals(1, leafDepths.stream().distinct().count());
        }

        private void checkLeafDepths(BTreeNode<String, String> node, int depth, List<Integer> leafDepths) {
            if (node.isLeaf) {
                leafDepths.add(depth);
                return;
            }

            for (BTreeNode<String, String> child : node.children) {
                checkLeafDepths(child, depth + 1, leafDepths);
            }
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 35, 50})
        void testAllNonLeafNodesHaveExactlyOnLessKeysThanChild(int numKeys) {
            for (int i = 1; i <= numKeys; i++) {
                dataStore.put(String.format("%d", i), String.format("value%d", i));
            }

            BTreeNode<String, String> rootNode = dataStore.getRoot();

            dataStore.printTree();

            // Every node, except for the root and the leaves, has at least ⌈m/2⌉ children.
            traverseAndExecute(rootNode, node -> {
                if (!node.isLeaf) {
                    assertEquals(node.children.size(), node.data.size() + 1);
                }
            });
        }
    }

    @Test
    void testKeysAreInSortedOrder() {
        // Insert keys in random order
        dataStore.put("key5", "value5");
        dataStore.put("key3", "value3");
        dataStore.put("key7", "value7");
        dataStore.put("key1", "value1");
        dataStore.put("key9", "value9");

        // Verify in-order traversal produces sorted order
        BTreeNode<String, String> root = dataStore.getRoot();
        List<DataRecord<String, String>> allKeys = inOrderTraversal(root);

        for (int i = 0; i < allKeys.size() - 1; i++) {
            assertTrue(allKeys.get(i).key().compareTo(allKeys.get(i + 1).key()) < 0,
                    "Keys should be in sorted order");
        }
    }

    @Test
    void testChildrenAreCorrectlyDistributedAfterSplit() {
        dataStore.put("key1", "value1");
        dataStore.put("key2", "value2");
        dataStore.put("key3", "value3");
        dataStore.put("key4", "value4");
        dataStore.put("key5", "value5");
        dataStore.put("key6", "value6");

        BTreeNode<String, String> root = dataStore.getRoot();

        dataStore.printTree();

        assertEquals(1, root.data.size(), "Root should have 1 key after split");
        assertEquals(2, root.children.size(), "Root should have 2 children after split");

        // Verify children have correct keys
        BTreeNode<String, String> leftChild = root.children.get(0);
        BTreeNode<String, String> rightChild = root.children.get(1);

        assertTrue(leftChild.data.size() <= ORDER - 1, "Left child should not exceed maximum keys");
        assertTrue(rightChild.data.size() <= ORDER - 1, "Right child should not exceed maximum keys");

        for (DataRecord<String, String> kv : leftChild.data) {
            assertTrue(kv.key().compareTo(root.data.getFirst().key()) < 0,
                    "Left child keys should be less than root key");
        }

        for (DataRecord<String, String> kv : rightChild.data) {
            assertTrue(kv.key().compareTo(root.data.getFirst().key()) > 0,
                    "Right child keys should be greater than root key");
        }
    }

    private List<DataRecord<String, String>> inOrderTraversal(BTreeNode<String, String> node) {
        List<DataRecord<String, String>> result = new ArrayList<>();
        if (node == null) return result;

        for (int i = 0; i < node.data.size(); i++) {
            if (!node.isLeaf) {
                result.addAll(inOrderTraversal(node.children.get(i)));
            }
            result.add(node.data.get(i));
        }

        if (!node.isLeaf) {
            result.addAll(inOrderTraversal(node.children.get(node.data.size())));
        }

        return result;
    }

    private void traverseAndExecute(BTreeNode<String, String> node, Consumer<BTreeNode<String, String>> action) {
        action.accept(node);

        if (!node.isLeaf) {
            for (BTreeNode<String, String> child : node.children) {
                traverseAndExecute(child, action);
            }
        }
    }
}
