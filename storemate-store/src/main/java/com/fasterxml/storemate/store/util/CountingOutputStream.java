package com.fasterxml.storemate.store.util;

import java.io.*;

import com.fasterxml.storemate.shared.hash.IncrementalHasher32;

/**
 * Simple helper class that counts number of bytes written; and
 * can optionally also calculate checksum on the fly.
 */
public class CountingOutputStream extends OutputStream
{
    protected final OutputStream _out;
    
    protected long _count = 0;

    protected final IncrementalHasher32 _hasher;
    
    public CountingOutputStream(OutputStream out) {
        this(out, null);
    }

    public CountingOutputStream(OutputStream out, IncrementalHasher32 hasher) {
        this._out = out;
        _hasher = hasher;
    }

    public long count() { return _count; }
    
    @Override
    public void close() throws IOException {
        _out.close();
    }

    @Override
    public void flush() throws IOException {
        _out.flush();
    }
    
    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len)  throws IOException {
        _count += len;
        _out.write(b, off, len);
        if (_hasher != null) {
            _hasher.update(b, off, len);
        }
    }

    byte[] TMP = null;
    
    @Override
    public void write(int b) throws IOException {
        if (TMP == null) {
            TMP = new byte[1];
        }
        TMP[0] = (byte) b;
        write(TMP, 0, 1);
    }

    public int calculateHash() {
        return _hasher.calculateHash();
    }
}
