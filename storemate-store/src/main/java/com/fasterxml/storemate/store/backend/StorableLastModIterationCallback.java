package com.fasterxml.storemate.store.backend;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.StoreException;

public abstract class StorableLastModIterationCallback extends StorableIterationCallback
{
    /**
     * Method that gets called first, before decoding key for the
     * primary entry or its data, to determine if an entry reached
     * via "last-modified" index should be processed or not.
     */
    public abstract IterationAction verifyTimestamp(long timestamp);

    /**
     * Method called during race condition, when entry that was 'validated'
     * (via index) is not found from main data; presumably due to race
     * condition. This call is done to make it possible to handle this as
     * it may indicate an actual problem of some kind.
     * Only one of this and {@link #processEntry} is ever called.
     *<p>
     * Default implementation is basically no-operation, defined for backwards
     * compatibility
     */
    public IterationAction processMissingEntry(StorableKey entryKey)
        throws StoreException
    {
        // anything but TERMINATE_ITERATION
        return IterationAction.SKIP_ENTRY;
    }
}
