package com.fasterxml.storemate.store.util;

import java.io.*;

import com.fasterxml.storemate.shared.hash.IncrementalHasher32;

/**
 * Wrapper for {@link InputStream} that keeps track of number of bytes
 * read and skipped. It can also optionally calculate checksum over
 * read data.
 * 
 * @since 0.9.12
 */
public class CountingInputStream extends InputStream
{
    protected final InputStream _in;
    protected final IncrementalHasher32 _hasher;
    
    protected long _readCount = 0;

    protected long _skipCount = 0;

    public CountingInputStream(InputStream src) {
        _in = src;
        _hasher = null;
    }

    public CountingInputStream(InputStream src, IncrementalHasher32 hasher)
    {
        _in = src;
        _hasher = hasher;
    }

    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */

    /**
     * Accessor for finding number of bytes read from this stream
     */
    public long readCount() { return _readCount; }

    /**
     * Accessor for finding number of bytes skipped using this stream
     */
    public long skipCount() { return _skipCount; }
    
    /**
     * Method for accessing {@link IncrementalHasher32} instance stream was
     * constructed with, if any.
     */
    public IncrementalHasher32 getHasher() {
        return _hasher;
    }
    
    /*
    /**********************************************************************
    /* InputStream implementation
    /**********************************************************************
     */

    @Override
    public int available() throws IOException {
        return _in.available();
    }

    @Override
    public void close() throws IOException {
        _in.close();
    }

    @Override
    public void mark(int readlimit) {
        _in.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
        return _in.markSupported();
    }

    protected byte[] TMP = null;
    
    @Override
    public int read() throws IOException {
        int c = _in.read();
        if (c >= 0) {
            ++_readCount;
            if (_hasher != null) {
                if (TMP == null) {
                    TMP = new byte[1];
                }
                TMP[0] = (byte) c;
                _hasher.update(TMP, 0, 1);
            }
        }
        return c;
    }

    @Override
    public final int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int offset, int len) throws IOException
    {
        int count = _in.read(b, offset, len);
        if (count > 0) {
            _readCount += count;
            if (_hasher != null) {
                _hasher.update(b, offset, count);
            }
        }
        return count;
    }

    @Override
    public void reset() throws IOException {
        _in.reset();
    }

    @Override
    public long skip(long n) throws IOException {
        long l = _in.skip(n);
        if (l > 0L) {
            _skipCount += l;
        }
        return l;
    }
}
