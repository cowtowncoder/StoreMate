package com.fasterxml.storemate.shared.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.zip.Checksum;

/**
 * Helper class that is similar to {@link java.io.ByteArrayOutputStream}
 * in usage, but bit more efficient for use cases StoreMate has.
 * Instead of a single byte array, underlying storage is a List of
 * arrays (that is, segmented virtual array). The biggest chunk is
 * also recycled as necessary using {@link BufferRecycler})
 *<p>
 * Note that instances are NOT designed to be reusable, since instance
 * creation is cheap as the underlying buffers are automatically recycled
 * as necessary. So do not try to be clever and reuse instances;
 * this will most likely not work, but instead rely on lower level byte buffer
 * recycling that occurs automatically, <b>as long as you call {@link #release}</b>
 * either directly or indirectly (note: {@link #close} will NOT call {@link #release}).
 */
public class ByteAggregator
    extends OutputStream
{
    protected final static byte[] NO_BYTES = new byte[0];

    protected final static int DEFAULT_MAX_BLOCK_SIZE = (1 << 18);
    
    protected final static int MIN_FIRST_BLOCK_SIZE = 0x1000; // 4k for first chunk

    protected final static int MIN_OTHER_BLOCK_SIZE = 0x4000; // 16k at least afterwards
    
    /**
     * Maximum block size we will use for individual non-aggregated
     * blocks. Let's limit to using 256k chunks.
     */
    protected int MAX_BLOCK_SIZE = DEFAULT_MAX_BLOCK_SIZE;

    /**
     * We can recycle parts of buffer, which is especially useful when dealing
     * with small content, but can also help reduce first-chunk overhead for
     * larger ones.
     */
    protected final static BufferRecycler _bufferRecycler = new BufferRecycler(MIN_FIRST_BLOCK_SIZE);

    protected final BufferRecycler.Holder _bufferHolder;

    protected LinkedList<byte[]> _pastBlocks = null;

    /**
     * Number of bytes within byte arrays in {@link _pastBlocks}.
     */
    protected int _pastLen;

    /**
     * Currently active processing block
     */
    protected byte[] _currBlock;

    protected int _currBlockPtr;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public ByteAggregator()
    {
        _bufferHolder = _bufferRecycler.getHolder();
        _currBlock = _bufferHolder.borrowBuffer();
        _currBlockPtr = 0;
    }

    public ByteAggregator(int minSize)
    {
        _bufferHolder = _bufferRecycler.getHolder();
        _currBlock = _bufferHolder.borrowBuffer(minSize);
        _currBlockPtr = 0;
    }
    
    public ByteAggregator(byte[] data, int offset, int len)
    {
        _bufferHolder = _bufferRecycler.getHolder();        
        _currBlock = _bufferHolder.borrowBuffer(len);
        _currBlockPtr = 0;
        System.arraycopy(data, offset, _currBlock, 0, len);
        _currBlockPtr = len;
    }

    public static ByteAggregator with(ByteAggregator aggr,
            byte[] data, int offset, int len)
    {
        if (aggr == null) {
            aggr = new ByteAggregator(data, offset, len);
        } else {
            aggr.write(data, offset, len);
        }
        return aggr;
    }

    /**
     * Alias for {@link #release}.
     * 
     * @deprecated Use {@link #release} instead
     */
    @Deprecated
    public void reset()
    {
        release();
    }
    
    /**
     * Method for clearing out all aggregated content, but <b>without</b>
     * returning all recyclable buffers, to make it possible to use
     * this instance efficiently.
     */
    public void resetForReuse()
    {
        _pastLen = 0;
        _currBlockPtr = 0;
        if (_pastBlocks != null) {
            _pastBlocks.clear();
        }
    }

    /**
     * Method for clearing out all aggregated content and return
     * recyclable buffers for reuse, if possible, rendering this
     * instance unusable for further operations.
     *<p>
     * Note that instance can NOT be used after this method is called;
     * instead, a new instance must be constructed.
     */
    public void release()
    {
        _pastLen = 0;
        _currBlockPtr = 0;
        if (_pastBlocks != null) {
            _pastBlocks.clear();
        }
        _bufferHolder.returnBuffer(_currBlock);
        _currBlock = null;
    }

    /*
    /**********************************************************************
    /* OutputStream implementation
    /**********************************************************************
     */

    @Override
    public final void write(byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public final void write(byte[] b, int off, int len)
    {
        while (true) {
            int max = _currBlock.length - _currBlockPtr;
            int toCopy = Math.min(max, len);
            if (toCopy > 0) {
                System.arraycopy(b, off, _currBlock, _currBlockPtr, toCopy);
                off += toCopy;
                _currBlockPtr += toCopy;
                len -= toCopy;
            }
            if (len <= 0) break;
            _allocMore();
        }
    }

    @Override
    public void write(int b) {
        if (_currBlockPtr >= _currBlock.length) {
            _allocMore();
        }
        _currBlock[_currBlockPtr++] = (byte) b;
    }

    @Override public void close() {
        /* Does nothing: should not call 'release' or 'resetForReuse', since content will
         * most likely be needed...
         */
    }

    @Override public void flush() { /* NOP */ }
    
    /*
    /**********************************************************************
    /* Access to contents
    /**********************************************************************
     */
    
    /**
     * Method called when results are finalized and we can get the
     * full aggregated result buffer to return to the caller.
     * Equivalent to:
     *<pre>
     *   toByteArray(true, null);
     *<pre>
     * that is, this also implicitly calls {@link #release} so that no
     * content is available for further calls; and no prefix will be
     * prepended.
     */
    public byte[] toByteArray()
    {
        return toByteArray(true, null);
    }
    
    /**
     * Method called when results are finalized and we can get the
     * full aggregated result buffer to return to the caller.
     * 
     * @param release Whether contents should be {@link #release} after
     *   the call or not
     */
    public byte[] toByteArray(boolean release)
    {
        return toByteArray(release, null);
    }

    /**
     * @param release Whether contents should be {@link #release} after
     *   the call or not
     * @param prefix Optional prefix to prepend before actual contents
     */
    public byte[] toByteArray(boolean release, byte[] prefix)
    {
        int totalLen = _pastLen + _currBlockPtr;
        if (prefix != null) {
            totalLen += prefix.length;
        }
        if (totalLen == 0) { // quick check: nothing aggregated?
            return NO_BYTES;
        }
        byte[] result = new byte[totalLen];
        int offset = 0;

        if (prefix != null) {
            final int len = prefix.length;
            if (len > 0) {
                System.arraycopy(prefix, 0, result, offset, len);
                offset += len;
            }
        }
        
        if (_pastBlocks != null && !_pastBlocks.isEmpty()) {
            for (byte[] block : _pastBlocks) {
                final int len = block.length;
                System.arraycopy(block, 0, result, offset, len);
                offset += len;
            }
        }
        System.arraycopy(_currBlock, 0, result, offset, _currBlockPtr);
        offset += _currBlockPtr;
        if (release) {
            release();
        }
        if (offset != totalLen) { // just a sanity check
            throw new RuntimeException("Internal error: total len assumed to be "+totalLen+", copied "+offset+" bytes");
        }
        return result;
    }

    /*
    /**********************************************************************
    /* Traversal/consumption methods
    /**********************************************************************
     */

    /**
     * Method for writing contents of this aggregator into provided
     * {@link OutputStream}
     */
    public void writeTo(OutputStream out) throws IOException
    {
        if (_pastBlocks != null && !_pastBlocks.isEmpty()) {
            for (byte[] block : _pastBlocks) {
                out.write(block, 0, block.length);
            }
        }
        final int len = _currBlockPtr;
        if (len > 0) {
            out.write(_currBlock, 0, len);
        }
    }

    /**
     * Method that can be used to access contents aggregated, by
     * getting aggregator to call {@link WithBytesCallback#withBytes} once
     * per each segment with data.
     */
    public <T> T withBytes(WithBytesCallback<T> callback)
    {
        T result = null;
        if (_pastBlocks != null && !_pastBlocks.isEmpty()) {
            for (byte[] block : _pastBlocks) {
                result = callback.withBytes(block, 0, block.length);
            }
        }
        final int len = _currBlockPtr;
        if (len > 0) {
            result = callback.withBytes(_currBlock, 0, len);
        }
        return result;
    }
    
    /**
     * Method for calculating {@link Checksum} over contents of
     * this aggregator.
     */
    public ByteAggregator calcChecksum(Checksum cs)
    {
        if (_pastBlocks != null && !_pastBlocks.isEmpty()) {
            for (byte[] block : _pastBlocks) {
                cs.update(block, 0, block.length);
            }
        }
        final int len = _currBlockPtr;
        if (len > 0) {
            cs.update(_currBlock, 0, len);
        }
        return this;
    }
    
    /*
    /**********************************************************************
    /* Other public methods
    /**********************************************************************
     */

    public final int size() {
        return _pastLen + _currBlockPtr;
    }

    /*
    /**********************************************************************
    /* Advanced functionality for "filling up" 
    /**********************************************************************
     */
    
    /**
     * Helper method for trying to read up to specified amount of content
     * into this aggregator.
     * Will try to read up to specified amount, and either read and return
     * that amount (if enough content available); or return number of bytes
     * read (if less content available).
     * 
     * @return Number of bytes actually read
     */
    public int readUpTo(InputStream in, final int maxLen)
        throws IOException
    {
        int readCount = 0;
        while (true) {
            final int leftToRead = maxLen - readCount;
            if (leftToRead <= 0) {
                break;
            }
            // find some space to read stuff into
            int free = _currBlock.length - _currBlockPtr;
            if (free == 0) {
                _allocMore();
                free = _currBlock.length - _currBlockPtr;
            }
            int count = in.read(_currBlock, 0, Math.min(free, leftToRead));
            if (count < 0) {
                break;
            }
            readCount += count;
            _currBlockPtr += count;
        }
        return readCount;
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    private void _allocMore()
    {
        _pastLen += _currBlock.length;

        /* Let's allocate block that's half the total size, except no smaller
         * than the initial block size (16k)
         */
        int newSize = Math.max((_pastLen >> 1), MIN_OTHER_BLOCK_SIZE);
        // plus not to exceed max we define...
        if (newSize > MAX_BLOCK_SIZE) {
            newSize = MAX_BLOCK_SIZE;
        }
        if (_pastBlocks == null) {
            _pastBlocks = new LinkedList<byte[]>();
        }
        _pastBlocks.add(_currBlock);
        _currBlock = new byte[newSize];
        _currBlockPtr = 0;
    }
}
