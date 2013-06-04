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
    /* Metadata, life-cycle
    /**********************************************************************
     */

    /**
     * Method that can be called to find if there are operations in-flight,
     * and if so, get the oldest timestamp from those operations.
     * This can be used to calculate high-water marks for traversing last-modified
     * index (to avoid accessing things modified after start of traversal).
     * Note that this only establishes conservative lower bound: due to race condition,
     * the oldest operation may finish before this method returns.
     * 
     * @return Timestamp of the "oldest" operation still being performed, if any,
     *      or 0L if none
     */
    public abstract long getOldestInFlightTimestamp();

    /**
     * Method that will count number of write operations in-flight
     * currently.
     */
    public abstract int getInFlightWritesCount();
    
    /*
    /**********************************************************************
    /* API, throttle methods for database access
    /**********************************************************************
     */
    
    public abstract Storable performGet(StoreOperationCallback<Storable> cb,
            long operationTime, StorableKey key)
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

    public static class Base
        extends StoreOperationThrottler
    {
        @Override
        public long getOldestInFlightTimestamp() { return 0L; }

        @Override
        public int getInFlightWritesCount() { return 0; }
        
        @Override
        public Storable performGet(StoreOperationCallback<Storable> cb,
                long operationTime, StorableKey key)
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
