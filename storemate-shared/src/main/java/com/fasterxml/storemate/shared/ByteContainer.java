package com.fasterxml.storemate.shared;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Simple read-only wrapper around basic in-heap byte array, used for buffering.
 */
public abstract class ByteContainer
{
    public final static ByteContainer emptyContainer() {
        return NoBytesContainer.instance;
    }

    public final static ByteContainer simpleContainer(byte[] bytes) {
        return simpleContainer(bytes, 0, bytes.length);
    }
    
    public final static ByteContainer simpleContainer(byte[] bytes, int offset, int len) {
        if (len <= 0) {
            return emptyContainer();
        }
        return new SimpleContainer(bytes, offset, len);
    }
    
    /**
     * Accessor for checking how many bytes are contained.
     */
    public abstract int byteLength();

    public abstract <T> T withBytes(WithBytesCallback<T> cb);
    
    /**
     * @param buffer Buffer into which copy bytes
     * @param offset Offset to copy bytes at
     * 
     * @return Offset after copying the bytes, that is: <code>offset + byteLength()</code>
     */
    public abstract int getBytes(byte[] buffer, int offset);

    public abstract void writeBytes(OutputStream out) throws IOException;

    private final static class NoBytesContainer extends ByteContainer
    {
        final static byte[] NO_BYTES = new byte[0];
        
        final static NoBytesContainer instance = new NoBytesContainer();
        
        private NoBytesContainer() { }

        @Override public int byteLength() {
            return 0;
        }

        @Override public int getBytes(byte[] buffer, int offset) {
            return offset;
        }

        @Override public void writeBytes(OutputStream out) throws IOException { }
        @Override public <T> T withBytes(WithBytesCallback<T> cb) {
            return cb.withBytes(NO_BYTES, 0, 0);
        }
    }

    private final static class SimpleContainer extends ByteContainer
    {
        private final byte[] _data;
        private final int _offset, _length;
        
        SimpleContainer(byte[] bytes, int offset, int len) {
            _data = bytes;
            _offset = offset;
            _length = len;
        }

        @Override public int byteLength() {
            return _length;
        }

        @Override public int getBytes(byte[] buffer, int offset) {
            System.arraycopy(_data, _offset, buffer, offset, _length);
            return (offset + _length);
        }

        @Override public void writeBytes(OutputStream out) throws IOException {
            out.write(_data, _offset, _length);
        }

        @Override public <T> T withBytes(WithBytesCallback<T> cb) {
            return cb.withBytes(_data, _offset, _length);
        }
    }
}
