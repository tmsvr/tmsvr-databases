package com.tmsvr.databases.lsmtree.sstable.bloomfilter.hash;

import org.apache.commons.codec.digest.MurmurHash3;

public class MurmurHashFunction implements HashFunction {

    @Override
    public int hash(byte[] input) {
        return MurmurHash3.hash32x86(input);
    }
}
