package com.fasterxml.storemate.store.impl;

import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StoreBackend;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.StoreOperationCallback;

/**
 * Helper class used for operations that do "GET, modify, PUT" style operation
 * that needs to be atomic.
 */
public abstract class ReadModifyOperationCallback<IN,OUT> 
    implements StoreOperationCallback<IN,OUT>
{
    @Override
    public OUT perform(StorableKey key, StoreBackend backend, IN arg)
            throws IOException, StoreException
    {
        Storable entry = backend.findEntry(key);
        return perform(key, backend, arg, entry);
    }

    /**
     * Method for sub-classes to implement
     * 
     * @param entry NOTE: may be null if no entry exists in the store
     */
    protected abstract OUT perform(StorableKey key, StoreBackend backend, IN arg,
            Storable entry)
        throws IOException, StoreException;
}
