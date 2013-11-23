package com.fasterxml.storemate.store.util;

import com.fasterxml.util.membuf.StreamyBytesMemBuffer;

/**
 * Interface used for "leasing" off-heap buffers, but with guarantees
 * that they actually get properly returned so they can be automatically
 * released.
 */
public interface ByteBufferCallback<T>
{
    /**
     * Main callback method called when a buffer is successfully
     * allocated.
     */
    public T withBuffer(StreamyBytesMemBuffer buffer);

    /**
     * Method called if allocation fails, usually due to insufficient
     * availability of segments
     */
    public T withError(IllegalStateException e);
}
