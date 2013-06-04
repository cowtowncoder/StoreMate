package com.fasterxml.storemate.store.util;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLongArray;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.*;

/**
 * {@link StoreOperationThrottler} that throttles all write operations
 * (usually PUT and DELETE) using N-way key-based partitions; such that
 * only a single active operation is allowed per partition.
 */
public class PartitionedWriteThrottler
    extends StoreOperationThrottler.Base
{
    private final static int MIN_PARTITIONS = 4;
    private final static int MAX_PARTITIONS = 256;
    
    protected final int _modulo;

    /**
     * If we are delegating to another throttler (instead of directly
     * calling callback), this is the throttler to delegate to.
     */
    protected final StoreOperationThrottler _delegatee;
    
    /**
     * Underlying semaphores used for locking
     */
    protected final Semaphore[] _semaphores;

    /**
     * We also need to keep track of requests in-flight, to be able to calculate
     * safe synchronization ranges.
     */
    protected final AtomicLongArray _inFlightStartTimes;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    /**
     * @param delegatee Object to which we forward actual 
     * @param n Minimum number of partitions (rounded up to next power of 2)
     * @param fair Whether underlying semaphores should be fair or not; fair ones have
     *   more overhead, but mostly (only?) for contested access, not uncontested
     */
    public PartitionedWriteThrottler(int n, boolean fair)
    {
        n = powerOf2(n);
        _modulo = n-1;
        _semaphores = new Semaphore[n];
        for (int i = 0; i < n; ++i) {
            _semaphores[i] = new Semaphore(1, fair);
        }
        _inFlightStartTimes = new AtomicLongArray(n);
        _delegatee = null;
    }

    protected PartitionedWriteThrottler(PartitionedWriteThrottler base,
            StoreOperationThrottler delegatee)
    {
        _modulo = base._modulo;
        _semaphores = base._semaphores;
        _inFlightStartTimes = base._inFlightStartTimes;
        _delegatee = delegatee;
    }

    private final static int powerOf2(int n)
    {
        if (n <= MIN_PARTITIONS) return MIN_PARTITIONS;
        if (n >= MAX_PARTITIONS) return MAX_PARTITIONS;
        int m = MIN_PARTITIONS;
        while (m < n) {
            m += m;
        }
        return m;
    }

    /*
    /**********************************************************************
    /* Public API, metadata etc
    /**********************************************************************
     */

    @Override
    public long getOldestInFlightTimestamp()
    {
        long lowest = Long.MAX_VALUE;
        for (int i = 0, last = _modulo; i <= last; ++i) {
            long timestamp = _inFlightStartTimes.get(i);
            if (timestamp > 0L) {
                if (timestamp < lowest) {
                    lowest = timestamp;
                }
            }
        }
        return (lowest == Long.MAX_VALUE) ? 0L : lowest;
    }

    @Override
    public int getInFlightWritesCount()
    {
        int count = 0;
        for (int i = 0, last = _modulo; i <= last; ++i) {
            long timestamp = _inFlightStartTimes.get(i);
            if (timestamp != 0L) {
                ++count;
            }
        }
        return count;
    }
    
    /*
    /**********************************************************************
    /* Public API
    /**********************************************************************
     */

    @Override
    public Storable performGet(StoreOperationCallback<Storable> cb, long operationTime,
            StorableKey key)
        throws IOException, StoreException
    {
        if (_delegatee != null) {
            return _delegatee.performGet(cb, operationTime, key);
        }
        // Not to be throttled, so:
        return cb.perform(operationTime, key, null);
    }

    @Override
    public StorableCreationResult performPut(StoreOperationCallback<StorableCreationResult> cb,
            long operationTime, StorableKey key, Storable value)
        throws IOException, StoreException
    {
        final int partition = _partitionFor(key);
        final Semaphore semaphore = _semaphores[partition];
        try {
            semaphore.acquire();
        } catch (InterruptedException e) { // could this ever occur?
            semaphore.release();
            throw new StoreException.Internal(key, e);
        }
        _inFlightStartTimes.set(partition, operationTime);
        try {
            if (_delegatee != null) {
                return _delegatee.performPut(cb, operationTime, key, value);
            }
            return cb.perform(operationTime, key, value);
        } finally {
            _inFlightStartTimes.set(partition, 0L);
            semaphore.release();
        }
    }

    @Override
    public Storable performSoftDelete(StoreOperationCallback<Storable> cb,
            long operationTime, StorableKey key)
        throws IOException, StoreException
    {
        final int partition = _partitionFor(key);
        final Semaphore semaphore = _semaphores[partition];
        try {
            semaphore.acquire();
        } catch (InterruptedException e) { // could this ever occur?
            semaphore.release();
            throw new StoreException.Internal(key, e);
        }
        _inFlightStartTimes.set(partition, operationTime);
        try {
            if (_delegatee != null) {
                return _delegatee.performSoftDelete(cb, operationTime, key);
            }
            return cb.perform(operationTime, key, null);
        } finally {
            _inFlightStartTimes.set(partition, 0L);
            semaphore.release();
        }
    }

    @Override
    public Storable performHardDelete(StoreOperationCallback<Storable> cb,
            long operationTime, StorableKey key)
        throws IOException, StoreException
    {
        final int partition = _partitionFor(key);
        final Semaphore semaphore = _semaphores[partition];
        try {
            semaphore.acquire();
        } catch (InterruptedException e) { // could this ever occur?
            semaphore.release();
            throw new StoreException.Internal(key, e);
        }
        _inFlightStartTimes.set(partition, operationTime);
        try {
            if (_delegatee != null) {
                return _delegatee.performHardDelete(cb, operationTime, key);
            }
            return cb.perform(operationTime, key, null);
        } finally {
            _inFlightStartTimes.set(partition, 0L);
            semaphore.release();
        }
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    private final int _partitionFor(StorableKey key)
    {
        /* NOTE: must shuffle key a bit, because lowest bits may also
         * be used for routing (that is, store may not handle keys with certain
         * ranges of Least Significant bits)
         */
        int hash = key.hashCode();
        hash ^= (int) (hash >>> 15);
        hash += (int) (hash >>> 7);
        return hash & _modulo;
    }
}
