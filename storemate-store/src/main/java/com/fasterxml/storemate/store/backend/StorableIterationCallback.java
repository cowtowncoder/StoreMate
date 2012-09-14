package com.fasterxml.storemate.store.backend;

import com.fasterxml.storemate.shared.*;

import com.fasterxml.storemate.store.*;

/**
 * API for objects used for iteration over
 * {@link Storable} entries a store has.
 */
public abstract class StorableIterationCallback
{
    /**
     * Method called for each entry, to check whether entry with the key
     * is to be processed.
     * 
     * @return Action to take for the entry with specified key
     */
    public abstract IterationAction verifyKey(StorableKey key);

    /**
     * Method called for each "accepted" entry (entry for which
     * {@link #verifKey} returned {@link IterationAction#PROCESS_ENTRY}).
     * 
     * @return Action to take; specifically, whether to continue processing
     *   or not (semantics for other values depend on context)
     */
    public abstract IterationAction processEntry(Storable entry)
        throws StoreException;
}
