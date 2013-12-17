package com.fasterxml.storemate.shared.hash;

import java.util.zip.Checksum;

/**
 * Base class for hashers that can work in incremental fashion, that is,
 * where content is sent via one or more calls to {@link #update}, before
 * final hash value is calculated.
 *<p>
 * Note: implementations are required to retain state so that it is possible
 * to calculate "partial" hash values throughout content, and call
 * {@link #calculateHash} multiple times, at different points. This is different
 * from many other hash calculation abstractions.
 *<p>
 * NOTE: since 0.9.21, this implements {@link Checksum}, for interoperability
 */
public abstract class IncrementalHasher32
    implements Checksum
{
    protected byte[] _singleByteArray;
    
    /**
     * Method for checking number of bytes that have been sent to
     * {@link #update} since creation or last call to {@link #reset}
     */
    public abstract long getLength();

    /**
     * Method for completing calculation of the hash value for the full
     * byte sequence fed with {@link #update}.
     * This will NOT reset state, so it is completely legal to feed more
     * content with {@link #update}, and calculate further hash codes
     * with same instance.
     *<p>
     * NOTE: since no state is reset with a call, calling this method typically
     * results in calculation to finalize state. So if you need to access
     * same hash value multiple times, call this method once and retain hash
     * value.
     */
    public abstract int calculateHash();

    @Override
    public abstract void reset();

    @Override
    public abstract void update(byte[] data, int offset, int len);

    @Override
    public long getValue() {
        return calculateHash();
    }

    @Override
    public void update(int b) {
        if (_singleByteArray == null) {
            _singleByteArray = new byte[1];
        }
        _singleByteArray[0] = (byte) b;
        update(_singleByteArray, 0, 1);
    }
}
