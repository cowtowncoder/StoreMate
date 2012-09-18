package com.fasterxml.storemate.shared.hash;

public abstract class BlockHasher32
{
    public final static int DEFAULT_SEED = 0;

    public final int hash(byte[] data) {
        return hash(DEFAULT_SEED, data, 0, data.length);
    }
    
    public int hash(int seed, byte[] data) {
        return hash(seed, data, 0, data.length);
    }

    public final int hash(byte[] data, int offset, int len) {
        return hash(DEFAULT_SEED, data, offset, len);
    }

    public abstract int hash(int seed, byte[] data, int offset, int len);
}
