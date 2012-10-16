package com.fasterxml.storemate.shared;

import java.util.Arrays;

import com.fasterxml.storemate.shared.hash.BlockHasher32;
import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;

public class StorableKey
{
    protected final byte[] _buffer;
    protected final int _offset, _length;

    /**
     * Hash code is calculated on-demand
     */
    protected int _hashCode;

    /**
     * Lazily constructed and stored instance of "refined" key
     * constructed from this raw key.
     */
    protected transient EntryKey _refinedKey;
    
    public StorableKey(byte[] buf) {
        this(buf, 0, buf.length);
    }

    public StorableKey(byte[] buf, int offset, int len)
    {
        _buffer = buf;
        _offset = offset;
        _length = len;
    }

    public final int length() { return _length; }

    @SuppressWarnings("unchecked")
    public <K extends EntryKey> K asEntryKey(EntryKeyConverter<K> conv)
    {
        EntryKey r = _refinedKey;
        if (r == null) {
            r = conv.rawToEntryKey(this);
            _refinedKey = r;
        }
        return (K) r;
    }
    
    public final byte[] asBytes() {
        if (_offset == 0) {
            if (_length == _buffer.length) {
                return _buffer;
            }
            return Arrays.copyOf(_buffer, _length);
        }
        return Arrays.copyOfRange(_buffer, _offset, _offset+_length);
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
