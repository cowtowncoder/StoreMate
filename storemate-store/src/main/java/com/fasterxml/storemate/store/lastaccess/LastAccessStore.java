package com.fasterxml.storemate.store.lastaccess;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.backend.*;

/**
 * Class that encapsulates optional storage of last-accessed
 * information, which implementations may choose to use for
 * things like dynamic expiration of not-recently-accessed entries.
 *<p>
 * Keys are derived from entry keys, so that grouped entries typically
 * map to a single entry, whereas individual entries just use
 * key as is or do not use last-accessed information at all.
 */
public abstract class LastAccessStore<K,E>
    implements com.fasterxml.storemate.shared.StartAndStoppable
{
    /*
    /**********************************************************************
    /* Life cycle
    /**********************************************************************
     */

    public LastAccessStore() {
    }

    /*
    /**********************************************************************
    /* StartAndStoppable dummy implementation
    /**********************************************************************
     */

    @Override
    public void start() { }

    @Override
    public abstract void prepareForStop();

    @Override
    public abstract void stop();

    /*
    /**********************************************************************
    /* Public API, metadata
    /**********************************************************************
     */

    public abstract boolean isClosed();
    
    /**
     * Method for checking whether link {@link #getEntryCount} has a method
     * to produce entry count using a method that is more efficient than
     * explicitly iterating over entries.
     * Note that even if true is returned, some amount of iteration may be
     * required, and operation may still be more expensive than per-entry access.
     */
    public abstract boolean hasEfficientEntryCount();

    /**
     * Accessor for getting approximate count of entries in the underlying
     * main entry database,
     * if (but only if) it can be accessed in
     * constant time without actually iterating over data.
     */
    public abstract long getEntryCount();

    /**
     * Accessor for backend-specific statistics information regarding
     * primary entry storage.
     * 
     * @param config Settings to use for collecting statistics
     */
    public abstract BackendStats getEntryStatistics(BackendStatsConfig config);

    /*
    /**********************************************************************
    /* Public API, basic lookups
    /**********************************************************************
     */

    public long findLastAccessTime(K key, LastAccessUpdateMethod method)
    {
        EntryLastAccessed entry = findLastAccessEntry(key, method);
        return (entry == null) ? 0L : entry.lastAccessTime;
    }

    public abstract long findLastAccessTime(E entry);

    public abstract EntryLastAccessed findLastAccessEntry(K key, LastAccessUpdateMethod method);

    /*
    /**********************************************************************
    /* Public API, updates
    /**********************************************************************
     */
    
    /**
     * Method called to update last-accessed information for given entry.
     * 
     * @param timestamp Actual last-accessed value
     */
    public abstract void updateLastAccess(E entry, long timestamp);

    /**
     * @return True if an entry was deleted; false otherwise (usually since there
     *    was no entry to delete)
     */
    public abstract boolean removeLastAccess(K key, LastAccessUpdateMethod method, long timestamp);

    /**
     * Alternate "raw" delete method, used when have a physical key; most commonly
     * during cleanup process.
     */
    public abstract boolean removeLastAccess(StorableKey rawKey);
    
    /*
    /**********************************************************************
    /* Public API, traversal
    /**********************************************************************
     */

    /**
     * Method for iterating over last-accessed entries, in an arbitrary order
     * (whatever is the most efficient way underlying store can expose
     * entries).
     */
    public abstract IterationResult scanEntries(LastAccessIterationCallback cb)
        throws StoreException;

    /*
    /**********************************************************************
    /* Helper classes for iteration (mostly to support cleanup)
    /**********************************************************************
     */

    /**
     * Callback for safe traversal over last-accessed entries; mostly needed
     * for clean up or statistics gathering operations.
     */
    public abstract static class LastAccessIterationCallback
    {
        /**
         * Method called for all entries.
         * 
         * @param key Raw key for last-accessed entry (which may be an actual entry key,
         *   or some transformation thereof)
         * 
         * @return Action to take; specifically, whether to continue processing
         *   or not (semantics for other values depend on context)
         */
        public abstract IterationAction processEntry(StorableKey key, EntryLastAccessed entry)
            throws StoreException;
    }
}
