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
    public abstract Storable performGet(StoreOperationCallback cb,
            long operationTime, StorableKey key)
        throws IOException, StoreException;

    public abstract Storable performPut(StoreOperationCallback cb,
            long operationTime, StorableKey key, Storable value)
        throws IOException, StoreException;

    public abstract Storable performSoftDelete(StoreOperationCallback cb,
            long operationTime, StorableKey key)
        throws IOException, StoreException;

    public abstract Storable performHardDelete(StoreOperationCallback cb,
            long operationTime, StorableKey key)
        throws IOException, StoreException;

    /*
    public abstract void performList(StoreOperationCallback cb);
    public abstract void performScan();
    */

    /**
     * Default non-throttling pass-through implementation: useful as a building block
     */
    public static class Base
        extends StoreOperationThrottler
    {
        @Override
        public Storable performGet(StoreOperationCallback cb,
                long operationTime, StorableKey key)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, null);
        }

        @Override
        public Storable performPut(StoreOperationCallback cb,
                long operationTime, StorableKey key, Storable value)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, value);
        }

        @Override
        public Storable performSoftDelete(StoreOperationCallback cb,
                long operationTime, StorableKey key)
            throws IOException, StoreException
        {
            return cb.perform(operationTime, key, null);
        }
            

        @Override
        public Storable performHardDelete(StoreOperationCallback cb,
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
    public abstract static class Delegating
        extends StoreOperationThrottler
    {
        protected final StoreOperationThrottler _throttler;

        public Delegating(StoreOperationThrottler t)
        {
            _throttler = t;
        }

        @Override
        public Storable performGet(StoreOperationCallback cb,
                long operationTime, StorableKey key)
            throws IOException, StoreException
        {
            return _throttler.performGet(cb, operationTime, key);
        }

        @Override
        public Storable performPut(StoreOperationCallback cb,
                long operationTime, StorableKey key, Storable value)
            throws IOException, StoreException
        {
            return _throttler.performPut(cb, operationTime, key, value);
        }

        @Override
        public Storable performSoftDelete(StoreOperationCallback cb,
                long operationTime, StorableKey key)
            throws IOException, StoreException
        {
            return _throttler.performSoftDelete(cb, operationTime, key);
        }
            

        @Override
        public Storable performHardDelete(StoreOperationCallback cb,
                long operationTime, StorableKey key)
            throws IOException, StoreException
        {
            return _throttler.performHardDelete(cb, operationTime, key);
        }
    }    
}
