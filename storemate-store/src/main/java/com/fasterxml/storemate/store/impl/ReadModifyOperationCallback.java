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
public abstract class ReadModifyOperationCallback
    implements StoreOperationCallback
{
    protected final StoreBackend _backend;

    public ReadModifyOperationCallback(StoreBackend backend) {
        _backend = backend;
    }
    
    @Override
    public final Storable perform(long time, StorableKey key, Storable value)
        throws IOException, StoreException
    {
        return modify(time, key, _backend.findEntry(key));
    }

    /**
     * Method for sub-classes to implement
     * 
     * @param entry NOTE: may be null if no entry exists in the store
     */
    protected abstract Storable modify(long time, StorableKey key,  Storable entry)
        throws IOException, StoreException;
}
