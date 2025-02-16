package com.tmsvr.databases.lsmtree.sstable.bloomfilter.hash;

public interface HashFunction {
    int hash(byte[] input);
}
