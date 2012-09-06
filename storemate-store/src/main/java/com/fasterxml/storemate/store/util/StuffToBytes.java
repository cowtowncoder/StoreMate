package com.fasterxml.storemate.store.util;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.WithBytesCallback;

/**
 * Reverse of {@link BytesToStuff}, used for encoding data
 */
public abstract class StuffToBytes
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
    /* Shared state
    /**********************************************************************
     */
    
    protected int _ptr;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    protected StuffToBytes() { }

    /**
     * Factory method for constructing actual "writer" instance; something
     * used for building serializations of data.
     */
    public static StuffToBytes writer(int maxLen) {
        return new Writer(maxLen);
    }

    /**
     * Factory method for building "estimator", thing that just counts upper
     * bound of bytes that would be produced, without actually doing
     * any encoding or copying.
     */
    public static StuffToBytes estimator() {
        return new Estimator();
    }

    /*
    /**********************************************************************
    /* API
    /**********************************************************************
     */

    public final int offset() {
        return _ptr;
    }
    
    public abstract StuffToBytes appendByte(byte b);
    public abstract StuffToBytes appendInt(int i);
    public abstract StuffToBytes appendLong(long l);

    public abstract StuffToBytes appendVInt(int i);
    public abstract StuffToBytes appendVLong(long l);
    
    public final StuffToBytes appendBytes(byte[] data) {
        return appendBytes(data, 0, data.length);
    }
    
    public abstract StuffToBytes appendBytes(byte[] data, int offset, int length);
    
    public abstract <T> T withResult(WithBytesCallback<T> cb);

    public abstract StuffToBytes appendLengthAndBytes(ByteContainer bytes);

    /**
     * Method for constructing actual serialization with appended data.
     * Will only work for writer, not estimate.
     */
    public abstract ByteContainer bufferedBytes();
    
    /*
    /**********************************************************************
    /* Implementations
    /**********************************************************************
     */

    protected static class Writer extends StuffToBytes
        implements WithBytesCallback<StuffToBytes>
    {
        /**
         * Buffer in which to append stuff (for real writers); if null, used in
         * "byte counting" mode
         */
        protected final byte[] _buffer;

        protected final int _end;
 
        protected Writer(int maxLen)
        {
            /* one safety measure: to allow for simpler appending of
             * VLongs and VInts, reserve bit of extra space.
             */
            maxLen += MAX_VLONG_LENGTH;
            _buffer = new byte[maxLen];
            _end = maxLen;
        }
        
        public <T> T withResult(WithBytesCallback<T> cb) {
            return cb.withBytes(_buffer,  0, _ptr);
        }

        /*
        /**********************************************************************
        /* API
        /**********************************************************************
         */

        @Override
        public ByteContainer bufferedBytes() {
            return ByteContainer.simple(_buffer, 0, _ptr);
        }

        @Override
        public StuffToBytes appendByte(byte b)
        {
            if (_ptr >= _end) {
                _reportBounds(1);
            }
            _buffer[_ptr++] = b;
            return this;
        }

        @Override
        public StuffToBytes appendLong(long l)
        {
            _verifyBounds(8);
            _appendInt((int) (l >>> 32));
            _appendInt((int) l);
            return this;
        }

        @Override
        public StuffToBytes appendInt(int i)
        {
            _verifyBounds(4);
            _appendInt(i);
            return this;
        }

        @Override
        public StuffToBytes appendVInt(int value)
        {
            if (value < 0) throw new IllegalArgumentException();
            // minor optimization for trivial case of single byte
            if (value <= 0x7F) {
                _buffer[_ptr++] = (byte) (value & 0x80);
                return this;
            }
            // otherwise, count length first
            final int start = _ptr;
            int end = start;
            int tmp = (value >>> 7);
            while (tmp > 0) {
                tmp >>>= 7;
                ++end;
            }
            _ptr = end+1;
            // and then write out
            _buffer[end] = (byte) ((value & 0x7F) | 0x80);
            do {
                value >>>= 7;
                _buffer[--end] = (byte) (value & 0x7F);
            } while (end > start);
            return this;
        }

        @Override
        public StuffToBytes appendVLong(long value)
        {
            if (value < 0L) throw new IllegalArgumentException();
            if (value < Integer.MAX_VALUE) {
                return appendVInt((int) value);
            }
            // so we know it's at least 5 bytes long... count exact length
            final int start = _ptr;
            int end = start+4;
            // and can downgrade to ints for counting rest of byte length
            int tmp = (int) (value >>> 35);
            while (tmp > 0) {
                tmp >>>= 7;
                ++end;
            }
            _ptr = end+1;
            // and then write out from end to beginning
            _buffer[end] = (byte) ((value & 0x7F) | 0x80);
            do {
                value >>>= 7;
                _buffer[--end] = (byte) (value & 0x7F);
            } while (end > start);
            return this;
        }

        @Override
        public StuffToBytes appendBytes(byte[] data, int offset, int length) {
            _verifyBounds(length);
            System.arraycopy(data, offset, _buffer, _ptr, length);
            _ptr += length;
            return this;
        }

        @Override
        public StuffToBytes appendLengthAndBytes(ByteContainer bytes)
        {
            if (bytes == null) { // nothing to add is same as byte[0] for us, so:
                appendVInt(0);
                return this;
            }
            appendVInt(bytes.byteLength());
            bytes.withBytes(this);
            return this;
        }

        @Override
        public StuffToBytes withBytes(byte[] buffer, int offset, int length) {
            return appendBytes(buffer, offset, length);
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

    /**
     * Implementation that merely counts size that would be taken if content
     * was written.
     */
    protected static class Estimator extends StuffToBytes
    {
        protected Estimator() { }

        public <T> T withResult(WithBytesCallback<T> cb) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteContainer bufferedBytes() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public StuffToBytes appendByte(byte b) {
            ++_ptr;
            return this;
        }

        @Override
        public StuffToBytes appendLong(long l) {
            _ptr += 8;
            return this;
        }

        @Override
        public StuffToBytes appendInt(int i)
        {
            _ptr += 4;
            return this;
        }

        @Override
        public StuffToBytes appendVInt(int i)
        {
            if (i <= 0x7F) {
                ++_ptr;
                return this;
            }
            do {
                i >>>= 7;
                ++_ptr;
            } while (i != 0);
            return this;
        }

        @Override
        public StuffToBytes appendVLong(long l)
        {
            if (l < Integer.MAX_VALUE) {
                return appendVInt((int) l);
            }
            _ptr += 5;
            int rem = (int) (l >>> 35);
            while (rem > 0) {
                rem >>>= 7;
                ++_ptr;
            }
            return this;
        }
        
        @Override
        public StuffToBytes appendBytes(byte[] data, int offset, int length) {
            _ptr += length;
            return this;
        }

        @Override
        public StuffToBytes appendLengthAndBytes(ByteContainer bytes)
        {
            if (bytes == null) {
                ++_ptr;
            } else {
                int len = bytes.byteLength();
                appendVInt(len);
                _ptr += len;
            }
            return this;
        }
    }
}
