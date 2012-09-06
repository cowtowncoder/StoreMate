package com.fasterxml.storemate.store.util;

import java.util.Arrays;

/**
 * Simple utility class for decoding semi-structured data.
 *<p>
 * All multi-byte values use Big-Endian byte ordering (MSB first);
 * VInts are assumed positive, last byte indicated by set sign-bit.
 */
public class BytesToStuff
{
    private final byte[] _data;
    private final int _end;

    private int _ptr;
    
    public BytesToStuff(byte[] data) {
        this(data, 0, data.length);
    }

    public BytesToStuff(byte[] data, int offset, int len)
    {
        _data = data;
        _ptr = offset;
        _end = offset+len;
    }

    /*
    /**********************************************************************
    /* Public API
    /**********************************************************************
     */
    
    public void skip(int count) {
        _verifyBounds(count);
        _ptr += count;
    }
    
    public int left() {
        return _end - _ptr;
    }
    
    public int offset() {
        return _ptr;
    }
    
    public byte nextByte()
    {
        if (_ptr >= _end) {
            _reportBounds(1);
        }
        return _data[_ptr++];
    }

    public byte[] nextBytes(int amount)
    {
        _verifyBounds(amount);
        final int ptr = _ptr;
        _ptr += amount;
        return Arrays.copyOfRange(_data, ptr, ptr+amount);
    }
    
    public int nextInt()
    {
        _verifyBounds(4);
        return _nextInt();
    }
    
    public long nextLong()
    {
        _verifyBounds(8);
        long l1 = _nextInt() << 32;
        long l2 = _nextInt();

        // this may look silly, but the thing is to avoid sign-extension...
        return l1 | ((l2 << 32) >>> 32);
    }

    public int nextVInt()
    {
        int bytesDone = 0;
        int ptr = _ptr;
        int value = 0;
        
        while (true) {
            ++bytesDone;
            if (ptr >= _end) {
                _reportBounds(bytesDone);
            }
            value = (value << 7);
            int i = _data[ptr++];
            if (i < 0) { // last one
                i &= 0x7F;
                value += i;
                break;
            }
            value += i;
        }
        _ptr = ptr;
        if (bytesDone > StuffToBytes.MAX_VINT_LENGTH) { // sanity check
            throw new IllegalStateException("Corrupt VInt, shouldn't have more than 5 bytes, had: "+bytesDone);
        }
        return value;
    }
    
    public long nextVLong()
    {
        int bytesDone = 0;
        int ptr = _ptr;
        long value = 0L;
        
        while (true) {
            ++bytesDone;
            if (ptr >= _end) {
                _reportBounds(bytesDone);
            }
            value = (value << 7);
            int i = _data[ptr++];
            if (i < 0) { // last one
                i &= 0x7F;
                value += i;
                break;
            }
            value += i;
        }
        _ptr = ptr;
        if (bytesDone > StuffToBytes.MAX_VLONG_LENGTH) { // only 9 (63 bits) because we don't do negative numbers
            throw new IllegalStateException("Corrupt VLong, shouldn't have more than 9 bytes, had: "+bytesDone);
        }
        return value;
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    private int _nextInt()
    {
        int ptr = _ptr;
        int i = (_data[ptr++] << 24);
        i |= (_data[ptr++] & 0xFF) << 16;
        i |= (_data[ptr++] & 0xFF) << 8;
        i |= (_data[ptr++] & 0xFF);
        _ptr = i;
        return i;
    }

    protected void _verifyBounds(int bytesNeeded)
    {
        if ((_ptr + bytesNeeded) > _end) {
            _reportBounds(bytesNeeded);
        }
    }
    
    protected void _reportBounds(int bytesNeeded)
    {
        int left = _end - _ptr;
        throw new IllegalStateException("Unexpected end of data: need "+bytesNeeded
                +"; only have "+left+" (offset "+_ptr+")");
    }
}
