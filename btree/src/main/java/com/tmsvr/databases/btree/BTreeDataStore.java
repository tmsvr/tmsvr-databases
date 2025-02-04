package com.tmsvr.databases.btree;

import com.tmsvr.databases.DataRecord;
import com.tmsvr.databases.DataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class BTreeDataStore<K extends Comparable<K>, V> implements DataStore<K, V> {
    private BTreeNode<K, V> root = new BTreeNode<>(true);

    @Override
    public void put(K key, V value) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Null key or value not allowed");
        }

        DataRecord<K, V> kv = new DataRecord<>(key, value);

        if (root.data.size() == BTreeNode.ORDER - 1) {
            // Root is full; split it and create a new root
            BTreeNode<K, V> newRoot = new BTreeNode<>(false);
            newRoot.children.add(root);
            newRoot.splitChild(0);
            root = newRoot;
        }
        root.insertNonFull(kv);
    }

    @Override
    public Optional<V> get(K key) {
        DataRecord<K, V> result = root.search(key);
        return result == null ? Optional.empty() : Optional.of(result.value());
    }

    @Override
    public void delete(K key) {
        if (root == null) {
            return;
        }

        root.deleteFromNode(key);

        // If the root becomes empty, and it is not a leaf, make the first child the new root
        if (root.data.isEmpty() && !root.isLeaf) {
            root = root.children.getFirst();
        }
    }

    BTreeNode<K, V> getRoot() {
        return root;
    }

    void printTree() {
        if (root == null) {
            log.info("The tree is empty.");
        } else {
            printTreeRecursively(root, 0);
        }
    }

    private void printTreeRecursively(BTreeNode<K, V> node, int level) {
        String indent = "  ".repeat(level);

        // Print the current node's keys
        System.out.print(indent + "Level " + level + " | Keys: ");
        for (DataRecord<K, V> kv : node.data) {
            System.out.print(kv.key() + ", ");
        }
        System.out.println();

        // Recursively print children
        if (!node.isLeaf) {
            for (BTreeNode<K, V> child : node.children) {
                printTreeRecursively(child, level + 1);
            }
        }
    }
}