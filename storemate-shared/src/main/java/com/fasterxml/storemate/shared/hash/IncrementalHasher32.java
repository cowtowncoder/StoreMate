package com.fasterxml.storemate.shared.hash;

/**
 * Base class for hashers that can work in incremental fashion, that is,
 * where content is sent via one or more calls to {@link #update}, before
 * final hash value is calculated.
 *<p>
 * Note: implementations are required to retain state so that it is possible
 * to calculate "partial" hash values throughout content, and call
 * {@link #getValue} multiple times, at different points. This is different
 * from many other hash calculation abstractions.
 */
public abstract class IncrementalHasher32
{
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

    public abstract void reset();

    public abstract void update(byte[] data, int offset, int len);
}
