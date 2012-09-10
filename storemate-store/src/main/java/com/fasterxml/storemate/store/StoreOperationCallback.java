package com.fasterxml.storemate.store;

import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Interface that defines callback used for allowing atomic operations
 * (usually of form "GET,modify,PUT") against store.
 * Locking only limits competing write methods (atomic and simple PUTs),
 * not read operations.
 *
 * @param <IN> Type of argument that is passed through
 * @param <OUT> Type of return value
 */
public interface StoreOperationCallback<IN,OUT>
{
    /**
     * Callback method called in context of write lock.
     * 
     * @param key Key of entry being modified
     * @param backend Backend store used for physical access
     * @param arg Optional argument to pass
     * 
     * @return Return value from operation, if any
     */
    public OUT perform(StorableKey key, StoreBackend backend, IN arg)
            throws IOException, StoreException;
}
