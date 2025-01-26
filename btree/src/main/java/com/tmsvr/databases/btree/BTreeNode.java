package com.tmsvr.databases.btree;

import com.tmsvr.databases.DataRecord;

import java.util.ArrayList;
import java.util.List;

class BTreeNode {
    static final int MIN_DEGREE = 3; // Minimum degree (t)
    static final int ORDER = MIN_DEGREE * 2; // Order (m)

    final List<DataRecord> data;
    final List<BTreeNode> children;
    final boolean isLeaf;

    public BTreeNode(boolean isLeaf) {
        this.isLeaf = isLeaf;
        this.data = new ArrayList<>(ORDER - 1);
        this.children = new ArrayList<>(ORDER);
    }

    // Search for a key in the subtree rooted at this node
    public DataRecord search(String key) {
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
    public void insertNonFull(DataRecord kv) {
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
            DataRecord existing = children.get(i).search(kv.key());
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
        BTreeNode fullChild = children.get(index);
        BTreeNode newChild = new BTreeNode(fullChild.isLeaf);

        // Middle key of the full child to be promoted
        DataRecord middleKey = fullChild.data.get(MIN_DEGREE - 1);

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
}
