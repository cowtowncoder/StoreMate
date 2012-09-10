package com.fasterxml.storemate.store;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Helper class for partitioning keyspace into single-access 'slices'
 * 
 */
public class StorePartitions
{
    private final static int MIN_PARTITIONS = 4;
    private final static int MAX_PARTITIONS = 1024;

    /**
     * Actual underlying data store
     */
    protected final StoreBackend _store;
    
    protected final int _modulo;

    /**
     * Underlying semaphores used for locking
     */
    protected final Semaphore[] _semaphores;

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
            _semaphores[i] = new Semaphore(1, true);
        }
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
     * @param cb Callback to call from locked context
     * @param arg Optional argument
     */
    public <IN,OUT> OUT withLockedPartition(StorableKey key, StoreOperationCallback<IN,OUT> cb, IN arg)
        throws IOException, StoreException
    {
        final Semaphore semaphore = _semaphores[_partitionFor(key)];
        try {
            semaphore.acquire();
        } catch (InterruptedException e) { // could this ever occur?
            throw new StoreException(key, e);
        }
        try {
            return cb.perform(key, _store, arg);
        } finally {
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
