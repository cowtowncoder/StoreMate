package com.fasterxml.storemate.shared.hash;

public abstract class BlockHasher32
{
    public final static int DEFAULT_SEED = 0;
    
    public int hash(int seed, byte[] data) {
        return hash(seed, data, 0, data.length);
    }

    public abstract int hash(int seed, byte[] data, int offset, int len);
}
