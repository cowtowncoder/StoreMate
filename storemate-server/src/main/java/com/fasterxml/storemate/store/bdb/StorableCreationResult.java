package com.fasterxml.storemate.store.bdb;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.Storable;

public class StorableCreationResult
{
    protected final boolean _succeeded;
    
    /**
     * Key of the entry that was to be inserted
     */
    protected final StorableKey _key;

    /**
     * In case there was an existing entry, it will be returned here
     */
    protected final Storable _prevEntry;
    
    public StorableCreationResult(StorableKey key, boolean success,
            Storable prevEntry)
    {
        _key = key;
        _succeeded = success;
        _prevEntry = prevEntry;
    }

    public boolean succeeded() { return _succeeded; }
    
    public StorableKey getKey() { return _key; }
    
    public Storable getPreviousEntry() { return _prevEntry; }
}
