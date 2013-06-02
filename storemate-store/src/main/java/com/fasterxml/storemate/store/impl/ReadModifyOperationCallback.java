package com.fasterxml.storemate.store.impl;

import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.StoreOperationCallback;
import com.fasterxml.storemate.store.backend.StoreBackend;

/**
 * Helper class used for operations that do "GET, modify, PUT" style operation
 * that needs to be atomic.
 */
public abstract class ReadModifyOperationCallback<IN,OUT> 
    implements StoreOperationCallback<IN,OUT>
{
    protected final StoreBackend _backend;

    public ReadModifyOperationCallback(StoreBackend backend) {
        _backend = backend;
    }
    
    @Override
    public OUT perform(StorableKey key, IN arg)
            throws IOException, StoreException
    {
        return perform(key, arg, _backend.findEntry(key));
    }

    /**
     * Method for sub-classes to implement
     * 
     * @param entry NOTE: may be null if no entry exists in the store
     */
    protected abstract OUT perform(StorableKey key, IN arg, Storable entry)
        throws IOException, StoreException;
}
