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
 * 
 * @since 0.9.9
 */
public abstract class StoreOperationThrottler
{
    /*
    /**********************************************************************
    /* API, throttle methods for database access
    /**********************************************************************
     */
    
    public abstract Storable performGet(StoreOperationSource source, long operationTime, StorableKey key,
            StoreOperationCallback<Storable> cb)
        throws IOException, StoreException;
    
    public abstract IterationResult performList(StoreOperationCallback<IterationResult> cb,
            long operationTime)
        throws IOException, StoreException;
    
    public abstract StorableCreationResult performPut(StoreOperationCallback<StorableCreationResult> cb,
            long operationTime, StorableKey key, Storable value)
        throws IOException, StoreException;

    public abstract Storable performSoftDelete(StoreOperationCallback<Storable> cb,
            long operationTime, StorableKey key)
        throws IOException, StoreException;

    public abstract Storable performHardDelete(StoreOperationCallback<Storable> cb,
            long operationTime, StorableKey key)
        throws IOException, StoreException;

    /*
    /**********************************************************************
    /* API, throttle methods for file system access
    /**********************************************************************
     */

    public abstract <OUT> OUT performFileRead(FileOperationCallback<OUT> cb,
            long operationTime, Storable value, File externalFile)
        throws IOException, StoreException;

    public abstract <OUT> OUT performFileWrite(FileOperationCallback<OUT> cb,
            long operationTime, StorableKey key, File externalFile)
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
        public Storable performGet(StoreOperationSource source,
                long operationTime, StorableKey key,
                StoreOperationCallback<Storable> cb)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, null);
        }
        
        @Override
        public IterationResult performList(StoreOperationCallback<IterationResult> cb,
                long operationTime)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, null, null);
        }
        
        @Override
        public StorableCreationResult performPut(StoreOperationCallback<StorableCreationResult> cb,
                long operationTime, StorableKey key, Storable value)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, value);
        }

        @Override
        public Storable performSoftDelete(StoreOperationCallback<Storable> cb,
                long operationTime, StorableKey key)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, null);
        }

        @Override
        public Storable performHardDelete(StoreOperationCallback<Storable> cb,
                long operationTime, StorableKey key)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, null);
        }
        
        @Override
        public <T> T performFileRead(FileOperationCallback<T> cb,
                long operationTime, Storable value, File externalFile)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, (value == null) ? null : value.getKey(),
                    value, externalFile);
        }

        @Override
        public <T> T performFileWrite(FileOperationCallback<T> cb,
                long operationTime, StorableKey key, File externalFile)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, null, externalFile);
        }
    }
}
