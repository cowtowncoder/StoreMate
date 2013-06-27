package com.fasterxml.storemate.store.util;

import com.fasterxml.util.membuf.StreamyBytesMemBuffer;

/**
 * Interface used for "leasing" off-heap buffers, but with guarantees
 * that they actually get properly returned so they can be automatically
 * released.
 */
public interface ByteBufferCallback<T>
{
    public T withBuffer(StreamyBytesMemBuffer buffer);
}
