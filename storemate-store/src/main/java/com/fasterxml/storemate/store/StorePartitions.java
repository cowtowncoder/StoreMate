package com.fasterxml.storemate.store;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLongArray;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.backend.StoreBackend;

/**
 * Helper class for partitioning keyspace into single-access 'slices'
 * 
 */
public class StorePartitions
{
    private final static int MIN_PARTITIONS = 4;
    private final static int MAX_PARTITIONS = 256;
    
    /**
     * Actual underlying data store
     */
    protected final StoreBackend _store;
    
    protected final int _modulo;

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
     * 
     * @param n Minimum number of partitions (rounded up to next power of 2)
     * @param fair Whether underlying semaphores should be fair or not; fair ones have
     *   more overhead, but mostly (only?) for contested access, not uncontested
     */
    public StorePartitions(StoreBackend store, int n, boolean fair)
    {
        _store = store;
        n = powerOf2(n);
        _modulo = n-1;
        _semaphores = new Semaphore[n];
        for (int i = 0; i < n; ++i) {
            _semaphores[i] = new Semaphore(1, fair);
        }
        _inFlightStartTimes = new AtomicLongArray(n);
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
    /* Public API
    /**********************************************************************
     */

    /**
     * Method to call to perform arbitrary multi-part atomic operations, guarded
     * by partitioned lock.
     * Note that since this method is essentially synchronized on N-way partitioned
     * mutex, it is essential that execution time is minimized to the critical
     * section.
     * 
     * @param key Entry being operated on
     * @param startTime Timestamp operation may use as "last-modified" timestamp; must not
     *    be greater than the current system time (virtual or real)
     * @param cb Callback to call from locked context
     * @param arg Optional argument
     */
    public Storable withLockedPartition(long startTime,
            StorableKey key, Storable value,
            StoreOperationCallback cb)
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
        _inFlightStartTimes.set(partition, startTime);
        try {
            return cb.perform(startTime, key, value);
        } finally {
            _inFlightStartTimes.set(partition, 0L);
            semaphore.release();
        }
    }

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

    /**
     * Method that will count number of operations in-flight
     * currently.
     */
    public int getInFlightCount()
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
