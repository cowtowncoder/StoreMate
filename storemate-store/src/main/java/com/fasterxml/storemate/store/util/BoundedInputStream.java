package com.fasterxml.storemate.store.util;

import java.io.*;

/**
 * Simple {@link InputStream} wrapper that ensures that caller can
 * read at most N bytes from the underlying stream.
 */
public class BoundedInputStream extends InputStream
{
    protected final InputStream _source;
    
    protected final long _maxReads;
    
    protected final boolean _delegateClose;

    protected long _currReads;
    
    public BoundedInputStream(InputStream source, long maxReads,
            boolean delegateClose)
    {
        _source = source;
        _maxReads = maxReads;
        if (_maxReads < 0L) throw new IllegalArgumentException();
        _delegateClose = delegateClose;
        _currReads = 0L;
    }

    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */

    public final long bytesRead() {
        return _currReads;
    }

    public final long bytesLeft() {
        return _maxReads - _currReads;
    }
    
    public final boolean isCompletelyRead() {
        return _currReads == _maxReads;
    }

    /*
    /**********************************************************************
    /* InputStream overrides
    /**********************************************************************
     */

    @Override
    public int available() throws IOException
    {
        int a = _source.available();
        long left = bytesLeft();
        if (a <= left) {
            return a;
        }
        return (int) left;
    }

    @Override
    public void close() throws IOException
    {
        if (_delegateClose) {
            _source.close();
        }
    }

    @Override
    public void mark(int readlimit) {
        throw new UnsupportedOperationException();
    }
    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public int read() throws IOException {
        if (isCompletelyRead()) {
            return -1;
        }
        ++_currReads;
        return _source.read();
    }

    @Override
    public int read(byte[] b)  throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        long max = bytesLeft();
        if (len > max) {
            len = (int) max;
            if (len == 0) {
                return -1;
            }
        }
        int actual = _source.read(b, off, len);
        if (actual > 0) {
            _currReads += actual;
        }
        return actual;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long skip(long n) throws IOException
    {
        long max = bytesLeft();
        if (n > max) {
            n = max;
            if (n == 0L) {
                return 0L;
            }
        }
        long actual = _source.skip(n);
        if (actual > 0L) {
            _currReads += actual;
        }
        return actual;
    }
}
