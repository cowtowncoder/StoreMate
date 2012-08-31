package com.fasterxml.storemate.store.util;

import com.fasterxml.storemate.shared.WithBytesCallback;

/**
 * Reverse of {@link BytesToStuff}, used for encoding data
 */
public final class StuffToBytes
{
    /*
    /**********************************************************************
    /* Shared constants
    /**********************************************************************
     */

    public final static int MAX_VLONG_LENGTH = 9;
    public final static int MAX_VINT_LENGTH = 5;

    /**
     * First data section is exactly 16 bytes long and fixed
     * (see [https://github.com/cowtowncoder/StoreMate/wiki/BDBDataFormat])
     */
    public final static int BASE_LENGTH = 16;

    /*
    /**********************************************************************
    /* State
    /**********************************************************************
     */

    protected final byte[] _buffer;

    protected final int _end;
    
    protected int _ptr;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public StuffToBytes(int maxLen)
    {
        _end = maxLen;
        _buffer = new byte[maxLen];
    }

    public int offset() {
        return _ptr;
    }
    
    public <T> T withResult(WithBytesCallback<T> cb) {
        return cb.withBytes(_buffer,  0, _ptr);
    }
    
    /*
    /**********************************************************************
    /* Writing API
    /**********************************************************************
     */

    public StuffToBytes appendByte(byte b) {
        if (_ptr >= _end) {
            _reportBounds(1);
        }
        _buffer[_ptr++] = b;
        return this;
    }

    public StuffToBytes appendLong(long l)
    {
        _verifyBounds(8);
        _appendInt((int) (l >>> 32));
        _appendInt((int) l);
        return this;
    }

    public StuffToBytes appendLong(int i)
    {
        _verifyBounds(4);
        _appendInt(i);
        return this;
    }
    
    public StuffToBytes append(byte[] data) {
        return append(data, 0, data.length);
    }

    public StuffToBytes append(byte[] data, int offset, int length) {
        _verifyBounds(length);
        System.arraycopy(data, offset, _buffer, _ptr, length);
        _ptr += length;
        return this;
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected void _appendInt(int i)
    {
        int ptr = _ptr;
        final byte[] buf = _buffer;
        
        buf[ptr++] = (byte) (i >> 24);
        buf[ptr++] = (byte) (i >> 16);
        buf[ptr++] = (byte) (i >> 8);
        buf[ptr++] = (byte) i;
        
        _ptr = ptr;
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
        throw new IllegalStateException("Buffer overrun: need "+bytesNeeded
                +"; only have "+left+" (offset "+_ptr+")");
    }
}
