package com.fasterxml.storemate.store.backend;

public abstract class StorableLastModIterationCallback extends StorableIterationCallback
{
    /**
     * Method that gets called first, before decoding key for the
     * primary entry or its data, to determine if an entry reached
     * via "last-modified" index should be processed or not.
     */
    public abstract IterationAction verifyTimestamp(long timestamp);
}
