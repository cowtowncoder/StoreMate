package com.fasterxml.storemate.shared;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Simple read-only wrapper around basic in-heap byte array, used for buffering.
 */
public abstract class ByteContainer
{
    public final static ByteContainer emptyContainer() {
        return NoBytesContainer.instance;
    }

    public final static ByteContainer simple(byte[] bytes) {
        return new SimpleContainer(bytes, 0, bytes.length);
    }
    
    public final static ByteContainer simple(byte[] bytes, int offset, int len)
    {
        if (len == 0) {
            return emptyContainer();
        }
        // Let's do some sanity checks, saves us debugging
        if (bytes == null) throw new IllegalArgumentException("Null 'bytes'");
        final int arrayLen = bytes.length;
        if (offset < 0 || len < 0 || (offset + len) > arrayLen) {
            throw new IllegalArgumentException("Illegal offset/length ("+offset+"/"+len
                    +"): extend beyond start/end of array of "+arrayLen);
        }
        return new SimpleContainer(bytes, offset, len);
    }
    
    /**
     * Accessor for checking how many bytes are contained.
     */
    public abstract int byteLength();

    /**
     * Simple accessor only to be used by tests (real code should use
     * callbacks or bulk access)
     */
    public abstract byte get(int index);
    
    /**
     * Factory method for creating a view of contents of this container.
     * 
     * @param offset Offset in the original buffer; has to be between
     *    0 and <code>length - 1</code> (inclusive)
     * @param length Length of the slice to create: must not extend past
     *   end of this instance
     *   
     * @return New container with the view, if necessary; or this instance
     *    if no change
     */
    public abstract ByteContainer view(int offset, int length);
    
    public abstract <T> T withBytes(WithBytesCallback<T> cb);

    public abstract <T> T withBytes(WithBytesCallback<T> cb, int offset, int length);
    
    /**
     * @param buffer Buffer into which copy bytes
     * @param offset Offset to copy bytes at
     * 
     * @return Offset after copying the bytes, that is: <code>offset + byteLength()</code>
     */
    public abstract int getBytes(byte[] buffer, int offset);
    
    public abstract byte[] asBytes();

    public abstract void writeBytes(OutputStream out) throws IOException;

    public abstract void writeBytes(OutputStream out, int offset, int length) throws IOException;
    
    private final static class NoBytesContainer extends ByteContainer
    {
        final static byte[] NO_BYTES = new byte[0];
        
        final static NoBytesContainer instance = new NoBytesContainer();
        
        private NoBytesContainer() { }

        @Override public int byteLength() {
            return 0;
        }

        @Override public byte get(int index) {
            throw new IllegalArgumentException("Bad offset ("+index+"); this length is 0");
        }
        
        @Override public int getBytes(byte[] buffer, int offset) {
            return offset;
        }

        @Override public byte[] asBytes() { return NO_BYTES; }

        @Override public void writeBytes(OutputStream out)  { }
        @Override
        public void writeBytes(OutputStream out, int offset, int length) {
        	if (offset != 0 || length != 0) {
                throw new IllegalArgumentException("Bad offset/length ("+offset+"/"+length+"); this length is 0");
        	}
        }

        @Override public <T> T withBytes(WithBytesCallback<T> cb) {
            return cb.withBytes(NO_BYTES, 0, 0);
        }
        
        @Override public <T> T withBytes(WithBytesCallback<T> cb, int offset, int length) {
            if (offset == 0 && length == 0) {
                return cb.withBytes(NO_BYTES, 0, 0);
            }
            throw new IllegalArgumentException("Bad offset/length ("+offset+"/"+length+"); this length is 0");
        }
        
        @Override
        public ByteContainer view(int offset, int length) {
            if (offset == 0 && length == 0) {
                return this;
            }
            throw new IllegalArgumentException("Bad offset/length ("+offset+"/"+length+"); this length is 0");
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

        @Override public byte get(int index) {
            if (index < 0 || index >= _length) {
                throw new IllegalArgumentException("Bad offset ("+index+"); this length is "+_length);
            }
            return _data[_offset + index];
        }
        
        @Override public int getBytes(byte[] buffer, int offset) {
            System.arraycopy(_data, _offset, buffer, offset, _length);
            return (offset + _length);
        }

        @Override
        public byte[] asBytes() {
            if (_offset == 0) {
                return Arrays.copyOf(_data, _length);
            }
            return Arrays.copyOfRange(_data, _offset, _offset + _length);
        }
        
        @Override public void writeBytes(OutputStream out) throws IOException {
            out.write(_data, _offset, _length);
        }

        @Override
        public void writeBytes(OutputStream out, int offset, int length) throws IOException
        {
        	if (offset < 0 || (offset+length) > _length) {
                throw new IllegalArgumentException("Bad offset/length ("+offset+"/"+length+"); this length is "+_length);
            }
            out.write(_data, _offset + offset, length);
        }
        
        @Override public <T> T withBytes(WithBytesCallback<T> cb) {
            return cb.withBytes(_data, _offset, _length);
        }

        @Override public <T> T withBytes(WithBytesCallback<T> cb, int offset, int length) {
            if (offset == 0 && length == _length) {
                return cb.withBytes(_data, _offset, _length);
            }
            if (offset < 0 || (offset+length) > _length) {
                throw new IllegalArgumentException("Bad offset/length ("+offset+"/"+length+"); this length is "+_length);
            }
            return cb.withBytes(_data, _offset + offset, length);
        }
        
        @Override
        public ByteContainer view(int offset, int length) {
            if (offset == 0 && length == _length) {
                return this;
            }
            if (offset < 0 || (offset+length) > _length) {
                throw new IllegalArgumentException("Bad offset/length ("+offset+"/"+length+"); this length is "+_length);
            }
            return new SimpleContainer(_data, _offset + offset, length);
        }
    }
}
