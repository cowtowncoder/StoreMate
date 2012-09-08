package com.fasterxml.storemate.store;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.backend.PhysicalStore;

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
    protected final PhysicalStore _store;
    
    protected final int _modulo;

    /**
     * Underlying semaphores used for locking
     */
    protected final Semaphore[] _semaphores;

    /**
     * 
     * @param n Minimum number of partitions (rounded up to next power of 2)
     * @param fair Whether underlying semaphores should be fair or not; fair ones have
     *   more overhead, but mostly (only?) for contested access, not uncontested
     */
    public StorePartitions(PhysicalStore store, int n, boolean fair)
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

    public StorableCreationResult put(StorableKey key, StorableCreationMetadata stdMetadata,
            Storable storable, boolean allowOverwrite)
        throws IOException, StoreException
    {
        /* NOTE: must shuffle key a bit, because lowest bits may also
         * be used for routing (that is, store may not handle keys with certain
         * ranges of Least Significant bits)
         */
        int hash = key.hashCode();
        hash ^= (int) (hash >>> 15);
        hash += (int) (hash >>> 7);
        
        final int partition = hash & _modulo;
        final Semaphore semaphore = _semaphores[partition];
        try {
            semaphore.acquire();
        } catch (InterruptedException e) { // could this ever occur?
            throw new StoreException(key, e);
        }
        try {
            return _store.putEntry(key, stdMetadata, storable, allowOverwrite);
        } finally {
            semaphore.release();
        }
    }
}
