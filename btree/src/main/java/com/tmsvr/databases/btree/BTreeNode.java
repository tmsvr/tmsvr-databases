package com.tmsvr.databases.btree;

class BTreeNode {
    String[] keys;        // Array to store keys
    BTreeNode[] children; // Array to store children nodes
    int numKeys;       // Number of keys in the node
    boolean isLeaf;    // Whether the node is a leaf node

    public BTreeNode(int t, boolean isLeaf) {
        this.isLeaf = isLeaf;
        keys = new String[2 * t - 1];  // Maximum keys (order 3, so t = 2)
        children = new BTreeNode[2 * t];  // Maximum children
        numKeys = 0;
    }
}
