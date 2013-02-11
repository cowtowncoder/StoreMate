package com.fasterxml.storemate.shared;

import java.util.Arrays;

import com.fasterxml.storemate.shared.hash.BlockHasher32;
import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;
import com.fasterxml.storemate.shared.util.WithBytesCallback;

public class StorableKey
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

    /*
    /**********************************************************************
    /* Simple accessories
    /**********************************************************************
     */

    public final int length() { return _length; }

    /**
     * Method that can be called to see whether this key has specified
     * key as a prefix, including possible case that keys are identical
     */
    public final boolean hasPrefix(StorableKey other)
    {
        if (other == this) return true;
        if (other == null) { // since meaning here would be ambiguous, let's fail instead
            throw new IllegalArgumentException("Can't pass null key");
        }
        final int prefixLen = other.length();
        if (prefixLen > _length) {
            return false;
        }
        // Optimization for common case of no offsets
        if (_offset == 0 && other._offset == 0) {
            return _equals(_buffer, other._buffer, prefixLen);
        }
        // otherwise offline
        return _equals(_buffer, _offset, other._buffer, other._offset,
                _offset + prefixLen);
    }
    
    /**
     * Method for checking whether raw contents of this key equal contents
     * of given byte sequence.
     */
    public final boolean equals(byte[] buffer, int offset, int length)
    {
        if (length != _length) {
            return false;
        }
        if (offset == 0 && _offset == 0) {
            return _equals(_buffer, buffer, length);
        }
        return _equals(_buffer, _offset, buffer, offset, length);
    }

    private final boolean _equals(byte[] b1, byte[] b2, int len)
    {
        int ptr = 0;
        while (ptr < len) {
            if (b1[ptr] != b2[ptr]) {
                return false;
            }
            ++ptr;
        }
        return true;
    }
    
    private final boolean _equals(byte[] b1, int ptr1, byte[] b2, int ptr2, int end)
    {
        while (ptr1 < end) {
            if (b1[ptr1] != b2[ptr2]) {
                return false;
            }
            ++ptr1;
            ++ptr2;
        }
        return true;
    }

    /*
    /**********************************************************************
    /* Find methods
    /**********************************************************************
     */

    public final int indexOf(byte toMatch) {
        return indexOf(toMatch, 0);
    }

    public final int indexOf(byte toMatch, int atOrAfter)
    {
        final byte[] b  = _buffer;
        for (int i = _offset+atOrAfter, end = _offset + _length; i < end; ++i) {
            if (b[i] == toMatch) {
                return i;
            }
        }
        return -1;
    }

    public final int lastIndexOf(byte toMatch) {
        return lastIndexOf(toMatch, 0);
    }

    public final int lastIndexOf(byte toMatch, int atOrAfter)
    {
        final byte[] b  = _buffer;
        final int first = _offset + atOrAfter;
        for (int i = _offset + _length; --i >= first; ) {
            if (b[i] == toMatch) {
                return i;
            }
        }
        return -1;
    }
    
    /*
    /**********************************************************************
    /* Mutant factories
    /**********************************************************************
     */
    
    public final byte[] asBytes() {
        if (_offset == 0) {
            if (_length == _buffer.length) {
                return _buffer;
            }
            return Arrays.copyOf(_buffer, _length);
        }
        return Arrays.copyOfRange(_buffer, _offset, _offset+_length);
    }

    public final StorableKey range(int offset, int length) {
        if (offset < 0 || length < 0 || (offset+length) > _length) {
            throw new IllegalArgumentException("Invalid range (offset "+offset+", length "+length
                    +"), for key with length of "+_length+" bytes");
        }
        if (offset == 0 && length == _length) {
            return this;
        }
        int from = _offset+offset;
        return new StorableKey(_buffer, from, length);
    }
    
    public final byte[] rangeAsBytes(int offset, int length)
    {
        if (offset < 0 || length < 0 || (offset+length) > _length) {
            throw new IllegalArgumentException("Invalid range (offset "+offset+", length "+length
                    +"), for key with length of "+_length+" bytes");
        }
        int from = _offset+offset;
        return Arrays.copyOfRange(_buffer, from, from + _length);
    }
    
    public final <T> T with(WithBytesCallback<T> cb) {
        return cb.withBytes(_buffer, _offset, _length);
    }

    public final <T> T withRange(WithBytesCallback<T> cb, int offset, int length) {
        if (offset < 0 || length < 0 || (offset+length) > _length) {
            throw new IllegalArgumentException("Invalid range (offset "+offset+", length "+length
                    +"), for key with length of "+_length+" bytes");
        }
        return cb.withBytes(_buffer, _offset+offset, length);
    }

    /*
    /**********************************************************************
    /* Hash code calculation
    /**********************************************************************
     */
    
    public int hashCode(BlockHasher32 hasher) {
        return hasher.hash(BlockHasher32.DEFAULT_SEED, _buffer, _offset, _length);
    }
    
    public int hashCode(BlockHasher32 hasher, int offset, int length) {
        if (offset < 0 || length < 0 || (offset+length) > _length) {
            throw new IllegalArgumentException("Invalid range (offset "+offset+", length "+length
                    +"), for key with length of "+_length+" bytes");
        }
        return hasher.hash(BlockHasher32.DEFAULT_SEED, _buffer, _offset+offset, length);
    }
    
    /*
    /**********************************************************************
    /* Std method overrides
    /**********************************************************************
     */
    
    @Override public int hashCode() {
        int h = _hashCode;
        if (h == 0) {
            h = BlockMurmur3Hasher.instance.hash(0, _buffer, _offset, _length);
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

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("[Key(");
        sb.append(_length);
        sb.append("), hash 0x");
        sb.append(Integer.toHexString(hashCode()));
        // TODO: add head/tail of key?
        sb.append(']');
        return sb.toString();
    }
}
