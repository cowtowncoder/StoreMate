package com.fasterxml.storemate.store;

import java.io.File;
import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.backend.IterationResult;

/**
 * Abstract class that defines interface StoreMate exposes to let higher-level
 * store implementations add specific throttling limits for low-level database
 * operations.
 *<p>
 * A standard implementation is provided as the default, so data stores need not
 * provide one if defaults work fine.
 */
public abstract class StoreOperationThrottler
{
    /*
    /**********************************************************************
    /* API, throttle methods for database access
    /**********************************************************************
     */

    /**
     * Similar to {@link #performGet}, except only checks for existence of entry with
     * given key.
     */
    public abstract Boolean performHas(StoreOperationSource source, long operationTime, StorableKey key,
            StoreOperationCallback<Boolean> cb)
        throws IOException, StoreException;
    
    public abstract Storable performGet(StoreOperationSource source, long operationTime, StorableKey key,
            StoreOperationCallback<Storable> cb)
        throws IOException, StoreException;
    
    public abstract IterationResult performList(StoreOperationSource source, long operationTime,
            StoreOperationCallback<IterationResult> cb)
        throws IOException, StoreException;
    
    public abstract StorableCreationResult performPut(StoreOperationSource source,
            long operationTime, StorableKey key, Storable value,
            StoreOperationCallback<StorableCreationResult> cb)
        throws IOException, StoreException;

    public abstract Storable performSoftDelete(StoreOperationSource source,
            long operationTime, StorableKey key,
            StoreOperationCallback<Storable> cb)
        throws IOException, StoreException;

    public abstract Storable performHardDelete(StoreOperationSource source,
            long operationTime, StorableKey key,
            StoreOperationCallback<Storable> cb)
        throws IOException, StoreException;

    /*
    /**********************************************************************
    /* API, throttle methods for file system access
    /**********************************************************************
     */

    public abstract <OUT> OUT performFileRead(StoreOperationSource source,
            long operationTime, Storable value, File externalFile,
            FileOperationCallback<OUT> cb)
        throws IOException, StoreException;

    public abstract <OUT> OUT performFileWrite(StoreOperationSource source,
            long operationTime, StorableKey key, File externalFile,
            FileOperationCallback<OUT> cb)
        throws IOException, StoreException;
    
    /*
    /**********************************************************************
    /* Standard implementation(s)
    /**********************************************************************
     */

    /**
     * Base implementation that simply calls callbacks without any throttling
     * or metrics tracking.
     */
    public static class Base
        extends StoreOperationThrottler
    {
        @Override
        public Boolean performHas(StoreOperationSource source, long operationTime, StorableKey key,
                StoreOperationCallback<Boolean> cb)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, null);
        }
        
        @Override
        public Storable performGet(StoreOperationSource source,
                long operationTime, StorableKey key,
                StoreOperationCallback<Storable> cb)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, null);
        }
        
        @Override
        public IterationResult performList(StoreOperationSource source,
                long operationTime,
                StoreOperationCallback<IterationResult> cb)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, null, null);
        }
        
        @Override
        public StorableCreationResult performPut(StoreOperationSource source,
                long operationTime, StorableKey key, Storable value,
                StoreOperationCallback<StorableCreationResult> cb)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, value);
        }

        @Override
        public Storable performSoftDelete(StoreOperationSource source,
                long operationTime, StorableKey key,
                StoreOperationCallback<Storable> cb)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, null);
        }

        @Override
        public Storable performHardDelete(StoreOperationSource source,
                long operationTime, StorableKey key,
                StoreOperationCallback<Storable> cb)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, null);
        }
        
        @Override
        public <T> T performFileRead(StoreOperationSource source,
                long operationTime, Storable value, File externalFile,
                FileOperationCallback<T> cb)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, (value == null) ? null : value.getKey(),
                    value, externalFile);
        }

        @Override
        public <T> T performFileWrite(StoreOperationSource source,
                long operationTime, StorableKey key, File externalFile,
                FileOperationCallback<T> cb)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, null, externalFile);
        }
    }
}
