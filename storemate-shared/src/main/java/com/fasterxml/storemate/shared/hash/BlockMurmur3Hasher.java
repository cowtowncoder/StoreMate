package com.fasterxml.storemate.shared.hash;

/**
 * Class that calculates full Murmur3 checksum for given data, using standard
 * Public Domain implementation from
 * https://github.com/yonik/java_util/blob/master/src/util/hash/MurmurHash3.java
 */
public final class BlockMurmur3Hasher extends BlockHasher32
{
    public final static BlockMurmur3Hasher instance = new BlockMurmur3Hasher();

    public final static int DEFAULT_SEED = 0;
    
    @Override
    public int hash(byte[] data) {
        return hash(DEFAULT_SEED, data, 0, data.length);
    }

    @Override
    public int hash(byte[] data, int offset, int length) {
        return hash(DEFAULT_SEED, data, offset, length);
    }
    
    public int hash(final int seed, byte[] data, int offset, int len)
    {
        int h1 = seed;
        int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

        for (int i=offset; i<roundedEnd; i+=4) {
	    	int k1 = (data[i] & 0xff) | ((data[i+1] & 0xff) << 8) | ((data[i+2] & 0xff) << 16) | (data[i+3] << 24);
	    	k1 *= IncrementalMurmur3Hasher.c1;
	    	k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
	    	k1 *= IncrementalMurmur3Hasher.c2;
	    	h1 ^= k1;
	    	h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
	    	h1 = h1 * 5 + IncrementalMurmur3Hasher.c3;
        }

        int k1 = 0;
        switch(len & 0x03) {
	      case 3:
	        k1 = (data[roundedEnd + 2] & 0xff) << 16;
	      case 2:
	        k1 |= (data[roundedEnd + 1] & 0xff) << 8;
	      case 1:
	        k1 |= (data[roundedEnd] & 0xff);
	        k1 *= IncrementalMurmur3Hasher.c1;
	        k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
	        k1 *= IncrementalMurmur3Hasher.c2;
	        h1 ^= k1;
        }

        // finalization
        h1 ^= len;

        // fmix(h1);
        h1 ^= h1 >>> 16;
	h1 *= 0x85ebca6b;
	h1 ^= h1 >>> 13;
	h1 *= 0xc2b2ae35;
	h1 ^= h1 >>> 16;

	return h1;
    }
}
