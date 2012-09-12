package com.fasterxml.storemate.shared.hash;

public abstract class BlockHasher32
{
    public int hash(byte[] data) {
        return hash(data, 0, data.length);
    }

    public abstract int hash(byte[] data, int offset, int len);
}
