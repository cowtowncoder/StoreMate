package com.fasterxml.storemate.store;

import com.fasterxml.storemate.shared.StorableKey;

public class StorableDeletionResult
{
    /**
     * Key of the entry that was to be inserted
     */
    protected final StorableKey _key;

    /**
     * In case there was or is an entry for the key, entry
     * after modifications (for soft delete) or prior to hard delete.
     */
    protected final Storable _entry;
    
    public StorableDeletionResult(StorableKey key, Storable entry)
    {
        _key = key;
        _entry = entry;
    }

    /**
     * Accessor for checking whether any action was taken: returns null
     * if there was no entry for given key; otherwise returns true.
     */
    public boolean hadEntry() { return (_entry != null); }
    
    public StorableKey getKey() { return _key; }
    
    public Storable getEntry() { return _entry; }

}
