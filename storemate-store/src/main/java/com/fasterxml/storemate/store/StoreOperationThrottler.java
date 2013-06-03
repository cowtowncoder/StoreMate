package com.fasterxml.storemate.store;

import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;

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
     * Factory method for constructing an instance that will delegate to given
     * throttler, instead of directly calling callback.
     */
    public abstract StoreOperationThrottler chainedInstance(StoreOperationThrottler delegating);

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
    /* API, actual throttle methods
    /**********************************************************************
     */
    
    public abstract Storable performGet(StoreOperationCallback<Storable> cb,
            long operationTime, StorableKey key)
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
    public abstract void performList(StoreOperationCallback cb);
    public abstract void performScan();
    */

    /*
    /**********************************************************************
    /* Standard implementation(s)
    /**********************************************************************
     */
    
    /**
     * Default non-throttling pass-through implementation: useful as a building block
     */
    public abstract static class Base
        extends StoreOperationThrottler
    {
        @Override
        public abstract StoreOperationThrottler chainedInstance(StoreOperationThrottler delegating);
        
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
    }

    /**
     * Implementation that by default simply forwards requests: useful for
     * adding throttling chains.
     */
    public static abstract class Delegating
        extends StoreOperationThrottler
    {
        protected final StoreOperationThrottler _throttler;

        public Delegating(StoreOperationThrottler t)
        {
            _throttler = t;
        }

        @Override
        public abstract StoreOperationThrottler chainedInstance(StoreOperationThrottler delegating);
        
        @Override
        public long getOldestInFlightTimestamp() {
            return _throttler.getOldestInFlightTimestamp();
        }

        @Override
        public int getInFlightWritesCount() {
            return _throttler.getInFlightWritesCount();
        }

        @Override
        public Storable performGet(StoreOperationCallback<Storable> cb,
                long operationTime, StorableKey key)
            throws IOException, StoreException
        {
            return _throttler.performGet(cb, operationTime, key);
        }

        @Override
        public StorableCreationResult performPut(StoreOperationCallback<StorableCreationResult> cb,
                long operationTime, StorableKey key, Storable value)
            throws IOException, StoreException
        {
            return _throttler.performPut(cb, operationTime, key, value);
        }

        @Override
        public Storable performSoftDelete(StoreOperationCallback<Storable> cb,
                long operationTime, StorableKey key)
            throws IOException, StoreException
        {
            return _throttler.performSoftDelete(cb, operationTime, key);
        }
            

        @Override
        public Storable performHardDelete(StoreOperationCallback<Storable> cb,
                long operationTime, StorableKey key)
            throws IOException, StoreException
        {
            return _throttler.performHardDelete(cb, operationTime, key);
        }
    }    
}
