package com.fasterxml.storemate.store;

import java.util.List;

/**
 * Extension of the core {@link StorableStore} API, which adds
 * methods useful for Admin tools, interfaces.
 */
public abstract class AdminStorableStore extends StorableStore
{
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
     * Method that can be used to access first {@link maxCount} entries
     * (in key order) from the store
     * 
     * @param maxCount Maximum number of entries to include.
     * @param includeDeleted Whether to include soft-deleted entries or not
     */
    public abstract List<Storable> dumpEntries(int maxCount, boolean includeDeleted)
        throws StoreException;
}
