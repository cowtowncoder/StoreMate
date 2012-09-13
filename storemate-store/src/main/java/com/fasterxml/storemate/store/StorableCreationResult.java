package com.fasterxml.storemate.store;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Value class returned from "PUT" (insert, upsert) operations,
 * to indicate actions taken.
 */
public class StorableCreationResult
{
    protected final boolean _succeeded;
    
    /**
     * Key of the entry that was to be inserted
     */
    protected final StorableKey _key;

    /**
     * Entry that was to be stored in the store: may or may not
     * be successfully put.
     */
    protected final Storable _newEntry;
    
    /**
     * In case there was an existing entry, it will be returned here
     */
    protected final Storable _prevEntry;
    
    public StorableCreationResult(StorableKey key, boolean success,
    		Storable newEntry, Storable prevEntry)
    {
        _key = key;
        _succeeded = success;
        _newEntry = newEntry;
        _prevEntry = prevEntry;
    }

    public boolean succeeded() { return _succeeded; }
    
    public StorableKey getKey() { return _key; }

    public Storable getNewEntry() { return _newEntry; }
    
    public Storable getPreviousEntry() { return _prevEntry; }
}
