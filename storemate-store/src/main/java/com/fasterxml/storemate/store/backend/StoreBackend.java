package com.fasterxml.storemate.store.backend;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.impl.StorableConverter;
import com.fasterxml.storemate.store.util.OverwriteChecker;

/**
 * Abstraction used by {@link StorableStore} for interacting with the
 * underlying physical database.
 */
public abstract class StoreBackend
{
    protected final Logger LOG;

    protected final StorableConverter _storableConverter;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    protected StoreBackend(StorableConverter storableConverter) {
        _storableConverter = storableConverter;
        LOG = LoggerFactory.getLogger(getClass());
    }

    protected StoreBackend(StorableConverter storableConverter, Logger log)
    {
        _storableConverter = storableConverter;
        LOG = log;
    }

    public abstract void start() throws Exception;
    public abstract void prepareForStop() throws Exception;
    public abstract void stop() throws Exception;

    /*
    /**********************************************************************
    /* Backend capabilities, metadata
    /**********************************************************************
     */

    /**
     * Method for checking whether link {@link #getEntryCount} has a method
     * to produce entry count using a method that is more efficient than
     * explicitly iterating over entries.
     * Note that even if true is returned, some amount of iteration may be
     * required, and operation may still be more expensive than per-entry access.
     */
    public abstract boolean hasEfficientEntryCount();

    /**
     * Method for checking whether link {@link #getIndexedCount} has a method
     * to produce index entry count using a method that is more efficient than
     * explicitly iterating over index entries.
     * Note that even if true is returned, some amount of iteration may be
     * required, and operation may still be more expensive than per-entry access.
     */
    public abstract boolean hasEfficientIndexCount();

    /**
     * Accessor for backend-specific statistics information regarding
     * primary entry storage.
     * 
     * @param config Settings to use for collecting statistics
     * 
     * @since 0.9.7
     */
    public abstract BackendStats getEntryStatistics(BackendStatsConfig config);

    /**
     * Accessor for backend-specific statistics information regarding
     * primary entry storage.
     * 
     * @param config Settings to use for collecting statistics
     * 
     * @since 0.9.7
     */
    public abstract BackendStats getIndexStatistics(BackendStatsConfig config);
    
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
     * main entry database,
     * if (but only if) it can be accessed in
     * constant time without actually iterating over data.
     */
    public abstract long getEntryCount();

    /**
     * Accessor for getting approximate count of entries accessible
     * via last-modifed index,
     * if (but only if) it can be accessed in
     * constant time without actually iterating over data.
     */
    public abstract long getIndexedCount();

    /**
     * Method that will iterate over entries and produce exact
     * count of entries. This should only be called from diagnostics
     * systems, and typically only if
     * {@link #getEntryCount} is not available (as per {@link #hasEfficientEntryCount}).
     */
    public abstract long countEntries() throws StoreException;

    /**
     * Method that will iterate over entries and produce exact
     * count of entries. This should only be called from diagnostics
     * systems, and typically only if
     * {@link #getIndexedCount} is not available (as per {@link #hasEfficientIndexCount}).
     */
    public abstract long countIndexed() throws StoreException;
    
    /*
    /**********************************************************************
    /* API, per-entry access, read
    /**********************************************************************
     */

    public abstract boolean hasEntry(StorableKey key) throws StoreException;

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

    /**
     * Method for scanning potentially all the entries in the store,
     * ordered by the primary key, starting with entry <b>after</b>
     * specified key
     *<p>
     * @since 0.8.8
     */
    public abstract IterationResult iterateEntriesAfterKey(StorableIterationCallback cb,
            StorableKey lastSeen)
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

    /**
     * Method that may insert (if no entry with given key exists) or
     * try to overwrite an entry with given key; but overwrite only happens
     * if given 'checker' object allows overwrite.
     * It should only be called if overwrite is truly conditional; otherwise
     * other methods (like {@link #putEntry}} or {@link #ovewriteEntry(StorableKey, Storable)})
     * are more efficient.
     * 
     * @param oldEntryRef If not null, existing entry will be returned via this object
     * 
     * @return True if entry was created or updated; false if not (an old entry
     *   exists, and checker did not allow overwrite)
     * 
     * @since 0.9.3
     */
    public abstract boolean upsertEntry(StorableKey key, Storable storable,
            OverwriteChecker checker, AtomicReference<Storable> oldEntryRef)
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
