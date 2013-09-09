package com.fasterxml.storemate.store.util;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLongArray;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.*;

/**
 * Object used to implement mutex for write operations
 * (usually PUT and DELETE) using N-way key-based partitions; such that
 * only a single active operation is allowed per partition.
 */
public class PartitionedWriteMutex
{
    private final static int MIN_PARTITIONS = 4;
    private final static int MAX_PARTITIONS = 256;
    
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
    /* Call back type(s)
    /**********************************************************************
     */

    /**
     * Callback used by partitioner.
     */
    public interface Callback<T> {
        public T performWrite(StorableKey key) throws IOException, StoreException;
    }
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    /**
     * @param n Minimum number of partitions (rounded up to next power of 2)
     * @param fair Whether underlying semaphores should be fair or not; fair ones have
     *   more overhead, but mostly (only?) for contested access, not uncontested
     */
    public PartitionedWriteMutex(int n, boolean fair)
    {
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
    /* Public API, metadata etc
    /**********************************************************************
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

    public <T> T partitionedWrite(long operationTime, StorableKey key, Callback<T> cb)
        throws IOException, StoreException
    {
        final int partition = _partitionFor(key);
        final Semaphore semaphore = _semaphores[partition];
        try {
            semaphore.acquire();
        } catch (InterruptedException e) { // could this ever occur?
            semaphore.release();
            throw new StoreException.Internal(key, "partitionedWrite() Semaphore-wait for "+key+" interrupted ("
                    +e.getClass().getName()+"), message: "+e.getMessage(),
                    e);
        }
        _inFlightStartTimes.set(partition, operationTime);
        try {
            return cb.performWrite(key);
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
