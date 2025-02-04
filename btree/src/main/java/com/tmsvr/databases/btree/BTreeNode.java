package com.tmsvr.databases.btree;

import com.tmsvr.databases.DataRecord;

import java.util.ArrayList;
import java.util.List;

class BTreeNode<K extends Comparable<K>, V> {
    static final int MIN_DEGREE = 3; // Minimum degree (t)
    static final int ORDER = MIN_DEGREE * 2; // Order (m)

    final List<DataRecord<K, V>> data;  // List of DataRecords with key of type K and value of type V
    final List<BTreeNode<K, V>> children;  // List of child nodes
    final boolean isLeaf;

    public BTreeNode(boolean isLeaf) {
        this.isLeaf = isLeaf;
        this.data = new ArrayList<>(ORDER - 1);
        this.children = new ArrayList<>(ORDER);
    }

    // Search for a key in the subtree rooted at this node
    public DataRecord<K, V> search(K key) {
        int i = 0;
        while (i < data.size() && key.compareTo(data.get(i).key()) > 0) {
            i++;
        }

        if (i < data.size() && key.equals(data.get(i).key())) {
            return data.get(i);
        }

        if (isLeaf) {
            return null;
        }

        return children.get(i).search(key); // Search in the appropriate child
    }

    // Insert a key-value pair into the subtree rooted at this node
    public void insertNonFull(DataRecord<K, V> kv) {
        int i = data.size() - 1;

        if (isLeaf) {
            // Check for duplicate keys and overwrite the value if found
            for (int j = 0; j < data.size(); j++) {
                if (data.get(j).key().equals(kv.key())) {
                    data.set(j, kv);
                    return;
                }
            }

            // Insert into the correct position in the sorted data list
            while (i >= 0 && kv.compareTo(data.get(i)) < 0) {
                i--;
            }
            data.add(i + 1, kv);
        } else {
            // Find the child where the key should be inserted
            while (i >= 0 && kv.compareTo(data.get(i)) < 0) {
                i--;
            }
            i++;

            // If the key exists in the child, update the value
            DataRecord<K, V> existing = children.get(i).search(kv.key());
            if (existing != null) {
                children.get(i).insertNonFull(kv);
                return;
            }

            // Split the child if it is full
            if (children.get(i).data.size() == ORDER - 1) {
                splitChild(i);
                if (kv.compareTo(data.get(i)) > 0) {
                    i++;
                }
            }
            children.get(i).insertNonFull(kv);
        }
    }

    // Split the child at the given index
    public void splitChild(int index) {
        BTreeNode<K, V> fullChild = children.get(index);
        BTreeNode<K, V> newChild = new BTreeNode<>(fullChild.isLeaf);

        // Middle key of the full child to be promoted
        DataRecord<K, V> middleKey = fullChild.data.get(MIN_DEGREE - 1);

        // Move the last (ORDER-1) keys from the full child to the new child
        newChild.data.addAll(fullChild.data.subList(MIN_DEGREE, fullChild.data.size()));
        fullChild.data.subList(MIN_DEGREE - 1, fullChild.data.size()).clear();

        // Move the last (ORDER) children from the full child to the new child
        if (!fullChild.isLeaf) {
            newChild.children.addAll(fullChild.children.subList(MIN_DEGREE, fullChild.children.size()));
            fullChild.children.subList(MIN_DEGREE, fullChild.children.size()).clear();
        }

        // Add the new child and the promoted key to the current node
        children.add(index + 1, newChild);
        data.add(index, middleKey);
    }

    public void deleteFromNode(K key) {
        int idx = findKeyIndex(key);

        if (idx < data.size() && data.get(idx).key().equals(key)) {
            if (isLeaf) {
                data.remove(idx); // Case 1: Key is in a leaf
            } else {
                // Case 2: Key is in an internal node
                if (children.get(idx).data.size() >= MIN_DEGREE) {
                    // Replace with predecessor
                    DataRecord<K, V> predecessor = getPredecessor(idx);
                    data.set(idx, predecessor);
                    children.get(idx).deleteFromNode(predecessor.key());
                } else if (children.get(idx + 1).data.size() >= MIN_DEGREE) {
                    // Replace with successor
                    DataRecord<K, V> successor = getSuccessor(idx);
                    data.set(idx, successor);
                    children.get(idx + 1).deleteFromNode(successor.key());
                } else {
                    // Merge both children
                    mergeChildren(idx);
                    children.get(idx).deleteFromNode(key);
                }
            }
        } else if (!isLeaf) {
            // Key is not in this node
            boolean lastChild = (idx == data.size());
            if (children.get(idx).data.size() < MIN_DEGREE) {
                fillChild(idx);
            }

            if (lastChild && idx > data.size()) {
                children.get(idx - 1).deleteFromNode(key);
            } else {
                children.get(idx).deleteFromNode(key);
            }
        }
    }

    private int findKeyIndex(K key) {
        int idx = 0;
        while (idx < data.size() && key.compareTo(data.get(idx).key()) > 0) {
            idx++;
        }
        return idx;
    }

    private DataRecord<K, V> getPredecessor(int idx) {
        BTreeNode<K, V> current = children.get(idx);
        while (!current.isLeaf) {
            current = current.children.get(current.data.size());
        }
        return current.data.getLast();
    }

    private DataRecord<K, V> getSuccessor(int idx) {
        BTreeNode<K, V> current = children.get(idx + 1);
        while (!current.isLeaf) {
            current = current.children.getFirst();
        }
        return current.data.getFirst();
    }

    private void mergeChildren(int idx) {
        BTreeNode<K, V> leftChild = children.get(idx);
        BTreeNode<K, V> rightChild = children.get(idx + 1);

        leftChild.data.add(data.remove(idx));
        leftChild.data.addAll(rightChild.data);

        if (!rightChild.isLeaf) {
            leftChild.children.addAll(rightChild.children);
        }

        children.remove(idx + 1);
    }

    private void fillChild(int idx) {
        if (idx > 0 && children.get(idx - 1).data.size() >= MIN_DEGREE) {
            borrowFromPrevious(idx);
        } else if (idx < data.size() && children.get(idx + 1).data.size() >= MIN_DEGREE) {
            borrowFromNext(idx);
        } else {
            if (idx < data.size()) {
                mergeChildren(idx);
            } else {
                mergeChildren(idx - 1);
            }
        }
    }

    private void borrowFromPrevious(int idx) {
        BTreeNode<K, V> child = children.get(idx);
        BTreeNode<K, V> sibling = children.get(idx - 1);

        child.data.addFirst(data.get(idx - 1));
        if (!child.isLeaf) {
            child.children.addFirst(sibling.children.removeLast());
        }

        data.set(idx - 1, sibling.data.removeLast());
    }

    private void borrowFromNext(int idx) {
        BTreeNode<K, V> child = children.get(idx);
        BTreeNode<K, V> sibling = children.get(idx + 1);

        child.data.add(data.get(idx));
        if (!child.isLeaf) {
            child.children.add(sibling.children.removeFirst());
        }

        data.set(idx, sibling.data.removeFirst());
    }
}
