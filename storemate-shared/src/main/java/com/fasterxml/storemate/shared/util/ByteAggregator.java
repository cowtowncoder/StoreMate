package com.fasterxml.storemate.shared.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;


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
 * this will most likely not work.
 */
public final class ByteAggregator
    extends OutputStream
{
    private final static byte[] NO_BYTES = new byte[0];

    private final static int MIN_FIRST_BLOCK_SIZE = 0x1000; // 4k for first chunk

    private final static int MIN_OTHER_BLOCK_SIZE = 0x4000; // 16k at least afterwards
    
    /**
     * Maximum block size we will use for individual non-aggregated
     * blocks. Let's limit to using 256k chunks.
     */
    private final static int MAX_BLOCK_SIZE = (1 << 18);

    /**
     * We can recycle parts of buffer, which is especially useful when dealing
     * with small content, but can also help reduce first-chunk overhead for
     * larger ones.
     */
    private final static BufferRecycler _bufferRecycler = new BufferRecycler(MIN_FIRST_BLOCK_SIZE);

    private final BufferRecycler.Holder _bufferHolder;
    
    private LinkedList<byte[]> _pastBlocks = null;

    /**
     * Number of bytes within byte arrays in {@link _pastBlocks}.
     */
    private int _pastLen;

    /**
     * Currently active processing block
     */
    private byte[] _currBlock;

    private int _currBlockPtr;

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
    
    protected void reset()
    {
        _pastLen = 0;
        _currBlockPtr = 0;
        if (_pastBlocks != null) {
            _pastBlocks.clear();
        }
        _bufferHolder.returnBuffer(_currBlock);
        _currBlock = null;
    }

    /**
     * Method called when results are finalized and we can get the
     * full aggregated result buffer to return to the caller.
     * Note that this also implicitly calls {@link #reset} so that no
     * content is available for further calls.
     */
    public byte[] toByteArray()
    {
        int totalLen = _pastLen + _currBlockPtr;
        
        if (totalLen == 0) { // quick check: nothing aggregated?
            return NO_BYTES;
        }
        
        byte[] result = new byte[totalLen];
        int offset = 0;

        if (_pastBlocks != null && !_pastBlocks.isEmpty()) {
            for (byte[] block : _pastBlocks) {
                
                int len = block.length;
                System.arraycopy(block, 0, result, offset, len);
                offset += len;
            }
        }
        System.arraycopy(_currBlock, 0, result, offset, _currBlockPtr);
        offset += _currBlockPtr;
        reset();
        if (offset != totalLen) { // just a sanity check
            throw new RuntimeException("Internal error: total len assumed to be "+totalLen+", copied "+offset+" bytes");
        }
        return result;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // OutputStream implementation
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public void write(byte[] b) {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len)
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

    @Override public void close() { /* NOP */ }

    @Override public void flush() { /* NOP */ }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Other public methods
    ///////////////////////////////////////////////////////////////////////
     */

    public final int size() {
        return _pastLen + _currBlockPtr;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Advanced functionality for "filling up" 
    ///////////////////////////////////////////////////////////////////////
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
    ///////////////////////////////////////////////////////////////////////
    // Internal methods
    ///////////////////////////////////////////////////////////////////////
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
