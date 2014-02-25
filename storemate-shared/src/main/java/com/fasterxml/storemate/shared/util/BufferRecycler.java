package com.fasterxml.storemate.shared.util;

import java.lang.ref.SoftReference;

/**
 * Simple container class that can be used for efficient per-thread
 * recycling of an individual byte buffer.
 * Note that an instance of this class MUST be used as a static member
 * of a class; otherwise per-thread semantics DO NOT WORK as expected,
 * and no recycling occurs.
 *<p>
 * The seeming complexity of this class is due to two levels of indirection
 * we need: one to add per-thread scoping, and the other for "soft references"
 * that allow JVM to collect recycled things as garbage, if and as necessary.
 * Because of this we need an intermediate holder object that caller can then
 * hold on to for life-cycle of its buffer: so basically caller just gets
 * holder once, gets a buffer if it needs one, as well as returns it back
 * to holder when it is done.
 */
public class BufferRecycler extends ThreadLocal<SoftReference<BufferRecycler.Holder>>
{
    private final int _initialBufferSize;
    
    public BufferRecycler(int initialSize) {
        _initialBufferSize = initialSize;
    }

    /**
     * Method used to get a reference to container object that actually
     * handles the details of buffer recycling.
     */
    public Holder getHolder()
    {
        SoftReference<Holder> ref = get();
        Holder h = (ref == null) ? null : ref.get();
        
        // Regardless of the reason we don't have holder, create replacement...
        if (h == null) {
            h = new Holder(_initialBufferSize);
            set(new SoftReference<Holder>(h));
        }
        return h;
    }
    
    /**
     * Simple container of actual recycled buffer instance
     */
    public static class Holder {
        private final int _initialBufferSize;
        private byte[] _buffer;

        public Holder(int initialSize) {
            _initialBufferSize = initialSize;
        }
        
        public byte[] borrowBuffer() {
            byte[] b = _buffer;
            if (b == null) {
                b = new byte[_initialBufferSize];
            } else {
                _buffer = null;
            }
            return b;
        }

        public byte[] borrowBuffer(int minSize)
        {
            byte[] b = _buffer;
            if (b == null) {
                b = new byte[Math.max(_initialBufferSize, minSize)];
            } else {
                _buffer = null;
                if (b.length < minSize) {
                    b = new byte[Math.max(_initialBufferSize, minSize)];
                }
            }
            return b;
        }
        
        public void returnBuffer(byte[] b) {
            if (_buffer != null) {
                // this is wrong, no matter what: return just once
                if (_buffer == b) {
                    throw new IllegalStateException("Trying to double-return a buffer (length: "+b.length+" bytes)");
                }
                // But is this necessary? Should not be, unless life-cycles overlap
                // so let's throw exception for now; re-visit if necessary
                throw new IllegalStateException("Trying to return a different buffer (had one with length "
                        +_buffer.length+" bytes, return one with "+b.length+" bytes)");
            }
            _buffer = b;
        }
    }
}
