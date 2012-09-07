package com.fasterxml.storemate.shared.hash;

import com.fasterxml.storemate.shared.hash.IncrementalHasher32;
import com.fasterxml.storemate.shared.hash.IncrementalMurmur3Hasher;

public class ChecksumUtil
{    
    public static int calcChecksum(byte[] data) {
        return calcChecksum(data, 0, data.length);
    }

    public static IncrementalHasher32 startChecksum()
    {
    	return new IncrementalMurmur3Hasher();
    }
    
    public static IncrementalHasher32 startChecksum(byte[] data, int offset, int len)
    {
    	IncrementalHasher32 hasher = startChecksum();
    	hasher.update(data, offset, len);
        return hasher;
    }
    
    public static int calcChecksum(byte[] data, int offset, int len) {
        return cleanChecksum(startChecksum(data, offset, len).calculateHash());
    }

    public static int getChecksum32(IncrementalHasher32 hasher)
    {
    	return cleanChecksum(hasher.calculateHash());
    }

    public static int cleanChecksum(int raw)
    {
        return (raw == 0) ? 1 : raw;
    }
}
