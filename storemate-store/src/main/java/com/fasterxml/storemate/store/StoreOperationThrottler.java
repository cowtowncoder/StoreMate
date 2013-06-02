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
    public abstract <IN,OUT> OUT performGet(StoreOperationCallback<IN,OUT> cb,
            long operationTime, StorableKey key, IN arg)
        throws IOException, StoreException;

    public abstract <IN,OUT> OUT performPut(StoreOperationCallback<IN,OUT> cb,
            long operationTime, StorableKey key, IN arg)
        throws IOException, StoreException;

    public abstract <IN,OUT> OUT performSoftDelete(StoreOperationCallback<IN,OUT> cb,
            long operationTime, StorableKey key, IN arg)
        throws IOException, StoreException;

    public abstract <IN,OUT> OUT performHardDelete(StoreOperationCallback<IN,OUT> cb,
            long operationTime, StorableKey key, IN arg)
        throws IOException, StoreException;

    /*
    public abstract void performList(StoreOperationCallback cb);
    public abstract void performScan();
    */

    /**
     * Default non-throttling pass-through implementation: useful as a building block
     */
    public abstract static class Base
        extends StoreOperationThrottler
    {
        @Override
        public <IN, OUT> OUT performGet(StoreOperationCallback<IN, OUT> cb,
                long operationTime, StorableKey key, IN arg)
            throws IOException, StoreException
        {
            return cb.perform(key, arg);
        }

        @Override
        public <IN, OUT> OUT performPut(StoreOperationCallback<IN, OUT> cb,
                long operationTime, StorableKey key, IN arg)
            throws IOException, StoreException
        {
            return cb.perform(key, arg);
        }

        @Override
        public <IN, OUT> OUT performSoftDelete(StoreOperationCallback<IN, OUT> cb,
                long operationTime, StorableKey key, IN arg)
            throws IOException, StoreException
        {
            return cb.perform(key, arg);
        }
            

        @Override
        public <IN, OUT> OUT performHardDelete(StoreOperationCallback<IN, OUT> cb,
                long operationTime, StorableKey key, IN arg)
            throws IOException, StoreException
        {
            return cb.perform(key, arg);
        }
    }

}
