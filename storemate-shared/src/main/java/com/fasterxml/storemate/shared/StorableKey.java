package com.fasterxml.storemate.shared;

import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;

public final class StorableKey
{
	protected final byte[] _buffer;
	protected final int _offset, _length;
	
	/**
	 * Hash code is calculated on-demand
	 */
	protected int _hashCode;

	public StorableKey(byte[] buf) {
		this(buf, 0, buf.length);
	}

	public StorableKey(byte[] buf, int offset, int len)
	{
		_buffer = buf;
		_offset = offset;
		_length = len;
	}

	public int length() { return _length; }

	public void with(WithBytesCallback cb) {
		cb.withBytes(_buffer, _offset, _length);
	}
	
	@Override public int hashCode() {
		int h = _hashCode;
		if (h == 0) {
			h = BlockMurmur3Hasher.hash(0, _buffer, _offset, _length);
			if (h == 0) { // need to mask 0 not to mean "not calculated"
				h = 1;
			}
			_hashCode = h;
		}
		return h;
	}

	@Override
    public boolean equals(Object o)
    {
    	if (o == this) return true;
    	if (o == null) return false;
    	if (o.getClass() != getClass()) return false;

    	StorableKey other = (StorableKey) o;
    	if (other._length != _length) return false;

    	final int end = _offset + _length;
    	final byte[] thisB = _buffer;
    	final byte[] thatB = other._buffer;
    	for (int i1 = _offset, i2 = other._offset; i1 < end; ) {
    		if (thisB[i1++] != thatB[i2++]) {
    			return false;
    		}
    	}
    	return true;
    }
}
