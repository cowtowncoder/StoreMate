package com.fasterxml.storemate.shared.hash;

public final class IncrementalMurmur3Hasher extends IncrementalHasher32
{
    protected final static int c1 = 0xcc9e2d51;
    protected final static int c2 = 0x1b873593;
	
    protected final static int c3 = 0xe6546b64;
    
	private final int _seed;

	/**
	 * Number of bytes for which checksum has been calculated
	 */
	private long _totalBytes;

	private int _partialBytes;
	private int _partialByteCount;
	
	/**
	 * Currently calculated partial hash value. Note that it will remain
	 * valid until {@link #reset} is called, so it is possible to calculate
	 * partial checksums with the same instance.
	 */
	private int _currentHash;

	public IncrementalMurmur3Hasher() {
		this(0);
	}

	public IncrementalMurmur3Hasher(int seed)
	{
		this._seed = seed;
		reset();
	}

    /*
    /**********************************************************************
    /* Public API
    /**********************************************************************
     */
	
	/**
	 * Method for checking number of bytes that have been sent to
	 * {@link #update} since creation or last call to {@link #reset}
	 */
	@Override
	public long getLength() {
		return _totalBytes;
	}
	
	@Override
	public int calculateHash()
	{
		/* Since we only retain 'partial' value, we now need to perform
		 * finalizations, as expected.
		 */
		int h1 = _currentHash;
		
		// First: any partial data ("tail") we may have needs to be added
		if (_partialByteCount > 0) {
			int k1 = _partialBytes;
	        k1 *= c1;
	        k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
	        k1 *= c2;
	        h1 ^= k1;
		}
		
	    // And then finalize the calculation
	    h1 ^= (int) (_totalBytes);

	    // fmix(h1);
	    h1 ^= h1 >>> 16;
	    h1 *= 0x85ebca6b;
	    h1 ^= h1 >>> 13;
	    h1 *= 0xc2b2ae35;
	    h1 ^= h1 >>> 16;

	    return h1;
	}
	
	@Override
	public void reset() {
		_partialBytes = _partialByteCount = 0;
		_totalBytes = 0L;
		_currentHash = _seed;
	}

	@Override
	public void update(byte[] data, int offset, int len)
	{
		if (data == null || offset < 0 || len < 1 || (offset+len) > data.length) {
			if (len == 0) {
				return;
			}
			throw new IllegalArgumentException();
		}
		
		// We will process all bytes, one way or another, so
		_totalBytes += len;
		
		// First things first: any partial data to use?
		if (_partialByteCount > 0) {
			int count = _resolvePartial(data, offset, len);
			if ((len -= count) == 0) {
				return;
			}
			offset += count;
		}
	
		int h1 = _currentHash;
	    int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

	    for (; offset < roundedEnd; offset += 4) {
	    	int k1 = _gatherIntLE(data, offset);
	    	k1 *= c1;
	    	k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
	    	k1 *= c2;
	    	h1 ^= k1;
	    	h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
	    	h1 = h1*5 + c3;
	    }
	    _currentHash = h1;
	    
	    // and save the tail, if any
	    int remainder = len&3;
	    if (remainder > 0) {
	    	_stashTail(data, offset, remainder);
	    }
	}

    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */
	
	/**
	 * Helper method for resolving existing partial data, before the
	 * fast main loop.
	 */
	private final int _resolvePartial(byte[] data, final int offset, final int len)
	{
		int ptr = offset;
		final int end = ptr+len;

		// may have 1, 2 or 3 bytes, in LSB
		switch (_partialByteCount) { // how many bytes have we stashed prior?
		case 1:
			if (ptr >= end) {
				return 0;
			}
			_partialBytes |= ((data[ptr++] & 0xFF) << 8);
			// fall through
		case 2:
			if (ptr >= end) {
				_partialByteCount = 2;
				return (ptr - offset);
			}
			_partialBytes |= ((data[ptr++] & 0xFF) << 16);
			// fall through
		case 3:
			if (ptr >= end) {
				_partialByteCount = 3;
				return (ptr - offset);
			}
			_partialBytes |= ((data[ptr++] & 0xFF) << 24);
			break;
		default:
			throw new IllegalStateException("Partial byte count = "+_partialByteCount+"; must be (1,3)");
		}
		
		// good: got a full int32, process:
		_partialByteCount = 0;
		int k1 = _partialBytes;
    	k1 *= c1;
    	k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
    	k1 *= c2;
    	int h1 = _currentHash;
    	h1 ^= k1;
    	h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
    	h1 = h1*5 + c3;
    	_currentHash = h1;
		
		return (ptr - offset);
	}
	
	/**
	 * Helper method for stashing partial data (1-3 bytes) after main
	 * loop.
	 */
	private final void _stashTail(byte[] data, int offset, int remainder)
	{
		_partialByteCount = remainder;
		int k1 = data[offset] & 0xFF;
		if (remainder > 1) {
	        k1 |= (data[++offset] & 0xff) << 8;
	        if (remainder > 2) {
	        	k1 |= (data[++offset] & 0xff) << 16;
	        }
		}
		_partialBytes = k1;
	}
	
	protected final static int _gatherIntLE(byte[] data, int index)
	{
	    int i = data[index] & 0xFF;
	    i |= (data[++index] & 0xFF) << 8;
	    i |= (data[++index] & 0xFF) << 16;
	    i |= (data[++index] << 24);
	    return i;
	}
}
