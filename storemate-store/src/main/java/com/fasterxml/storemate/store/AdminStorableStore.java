package com.fasterxml.storemate.store;

import java.io.IOException;
import java.util.List;

/**
 * Extension of the core {@link StorableStore} API, which adds
 * methods useful for Admin tools, interfaces.
 */
public abstract class AdminStorableStore extends StorableStore
{
    /**
     * Method for finding approximate number of currently in-flight (being processed)
     * database write operations.
     */
    public abstract int getInFlightWritesCount();

    /**
     * Method that can be used to scan over the <b>whole</b> store,
     * counting number of soft-deleted entries.
     * 
     * @param maxRuntimeMsecs Maximum time to run before failing
     * 
     * @throws StoreException For failures of the backend store
     * @throws IllegalStateException If maximum runtime is exceeded, will throw an exception
     */
    public abstract long getTombstoneCount(long maxRuntimeMsecs)
        throws IllegalStateException, StoreException;

    /**
     * Method that can be used to access first <code>maxCount</code> entries
     * (in key order) from the store
     * 
     * @param maxCount Maximum number of entries to include.
     * @param includeDeleted Whether to include soft-deleted entries or not
     */
    public abstract List<Storable> dumpEntries(int maxCount, boolean includeDeleted)
        throws StoreException;

    /**
     * Method that can be used to access first <code>maxCount</code> entries
     * (in 'last-modified' order, from oldest to newest) from the store
     * 
     * @param maxCount Maximum number of entries to include.
     * @param fromTime Oldest timestamp to include (inclusive); since timestamps
     *    are positive, use 0l for "all"
     * @param includeDeleted Whether to include soft-deleted entries or not
     */
    public abstract List<Storable> dumpOldestEntries(int maxCount, long fromTime,
            boolean includeDeleted)
        throws StoreException;

    public abstract int removeTombstones(int maxToRemove)
        throws IOException, StoreException;

    public abstract int removeEntries(final int maxToRemove)
        throws IOException, StoreException;
}
