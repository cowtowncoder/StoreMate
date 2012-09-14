package com.fasterxml.storemate.store.impl;

import java.util.*;

import com.fasterxml.storemate.shared.StorableKey;

import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.StorableIterationCallback;

public abstract class StorableCollector extends StorableIterationCallback 
{
    protected final int maxKeys;

    public int total = 0;

    protected final ArrayList<StorableKey> keys;
    
    public StorableCollector(int maxToCollect) {
        maxKeys = maxToCollect;
        keys = new ArrayList<StorableKey>(maxToCollect);
    }

    public List<StorableKey> getCollected() { return keys; }
    
    @Override public IterationAction verifyKey(StorableKey key) {
        ++total;
        return IterationAction.PROCESS_ENTRY;
    }

    @Override
    public IterationAction processEntry(Storable entry) {
        if (includeEntry(entry)) {
            keys.add(entry.getKey());
            if (keys.size() >= maxKeys) {
                return IterationAction.TERMINATE_ITERATION;
            }
        }
        return IterationAction.PROCESS_ENTRY;
    }

    public abstract boolean includeEntry(Storable entry);
}
