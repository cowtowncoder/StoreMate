package com.fasterxml.storemate.store.backend;

import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.impl.StorableConverter;

/**
 * Abstraction used by {@link StorableStore} for interacting with the
 * underlying physical database.
 */
public abstract class StoreBackend
{
    protected final StorableConverter _storableConverter;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    protected StoreBackend(StorableConverter storableConverter)
    {
        _storableConverter = storableConverter;
    }

    public abstract void start();
    public abstract void stop();

    /*
    /**********************************************************************
    /* API, accessors
    /**********************************************************************
     */

    public final StorableConverter getStorableConverter() {
        return _storableConverter;
    }
    
    /*
    /**********************************************************************
    /* API, metadata access
    /**********************************************************************
     */
    
    /**
     * Accessor for getting approximate count of entries in the underlying
     * main BDB database
     */
    public abstract long getEntryCount();

    /**
     * Accessor for getting approximate count of entries accessible
     * via last-modifed index.
     */
    public abstract long getIndexedCount();

    /*
    /**********************************************************************
    /* API Impl, read
    /**********************************************************************
     */

    public abstract boolean hasEntry(StorableKey key);

    public abstract Storable findEntry(StorableKey key) throws StoreException;

    /*
    /**********************************************************************
    /* API Impl, iteration
    /**********************************************************************
     */

    /**
     * Method for scanning potentially all the entries in the store,
     * in the fastest iteration order (usually derived from physical
     * storage ordering).
     *<p>
     * Note that iteration is not transactional, in that operations
     * may modify entries during iteration process.
     *   
     * @return True if iteration completed succesfully; false if it was terminated
     */
    public abstract IterationResult scanEntries(StorableIterationCallback cb)
        throws StoreException;

    /**
     * Method for scanning potentially all the entries in the store,
     * ordered by the primary key.
     *<p>
     * Note that iteration is not transactional, in that operations
     * may modify entries during iteration process.
     * 
     * @param cb Callback used for actual iteration
     * 
     * @return True if iteration completed successfully; false if it was terminated
     */
    public IterationResult iterateEntriesByKey(StorableIterationCallback cb)
        throws StoreException
    {
        return iterateEntriesByKey(cb, null);
    }
    
    /**
     * Method for scanning potentially all the entries in the store,
     * ordered by the primary key.
     *<p>
     * Note that iteration is not transactional, in that operations
     * may modify entries during iteration process.
     * 
     * @param cb Callback used for actual iteration
     * @param firstKey (optional) If not null, key for the first entry
     *   to include (inclusive); if null, starts from the very first entry
     *   
     * @return True if iteration completed successfully; false if it was terminated
     */
    public abstract IterationResult iterateEntriesByKey(StorableIterationCallback cb,
            StorableKey firstKey)
        throws StoreException;

    public IterationResult iterateEntriesByModifiedTime(StorableLastModIterationCallback cb)
        throws StoreException
    {
        return iterateEntriesByModifiedTime(cb, 0L);
    }
    
    /**
     * Method for scanning potentially all the entries in the store,
     * ordered by the last-modified order (from oldest to newest)
     *<p>
     * Note that iteration is not transactional, in that operations
     * may modify entries during iteration process.
     * 
     * @param cb Callback used for actual iteration
     * @param firstTimestamp (optional) Key for the first entry
     *   to include (inclusive); if null, starts from the very first entry
     *   
     * @return True if iteration completed successfully; false if it was terminated
     */
    public abstract IterationResult iterateEntriesByModifiedTime(StorableLastModIterationCallback cb,
            long firstTimestamp)
        throws StoreException;
    
    /*
    /**********************************************************************
    /* API Impl, insert/update
    /**********************************************************************
     */
    
    /**
     * Method that tries to create specified entry in the database,
     * if (and only if!) no entry exists for given key.
     * If an entry exists, it will be returned and no changes are made.
     * 
     * @return Null if creation succeeded; or existing entry if not
     */
    public abstract Storable createEntry(StorableKey key, Storable storable)
        throws IOException, StoreException;

    /**
     * Method that inserts given entry in the database, possibly replacing
     * an existing version; also returns the old entry.
     * 
     * @return Existing entry, if any
     */
    public abstract Storable putEntry(StorableKey key, Storable storable)
        throws IOException, StoreException;

    /**
     * Method that forcibly inserts or ovewrites entry for given key,
     * without trying to read possibly existing version. Should only
     * be used when modifications are made from an atomic operation
     * (to avoid race conditions).
     */
    public abstract void ovewriteEntry(StorableKey key, Storable storable)
        throws IOException, StoreException;
    
    /*
    /**********************************************************************
    /* API Impl, delete
    /**********************************************************************
     */

    /**
     * Method called to physically delete entry for given key, if one
     * exists.
     * 
     * @return False if deletion was not done because no such entry was found;
     *    true if deletion succeeeded
     * 
     * @throws StoreException If deletion failed to due to backend-specific problem
     * @throws IOException If deletion failed due to underlying I/O problem, exposed by
     *   store (not all stores expose them)
     */
    public abstract boolean deleteEntry(StorableKey key)
        throws IOException, StoreException;
}
