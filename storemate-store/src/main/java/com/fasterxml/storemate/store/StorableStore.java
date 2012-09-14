package com.fasterxml.storemate.store;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.storemate.shared.*;

import com.fasterxml.storemate.store.backend.StorableIterationCallback;
import com.fasterxml.storemate.store.file.FileManager;

/**
 * Simple abstraction for storing "decorated BLOBs", with a single
 * secondary index that can be used for traversing entries by
 * "last modified" time.
 */
public abstract class StorableStore
{
    /*
    /**********************************************************************
    /* Life-cycle methods
    /**********************************************************************
     */

    public abstract void start();
    
    public abstract void stop();
    
    /*
    /**********************************************************************
    /* API, simple accessors for state, helper objects
    /**********************************************************************
     */

    public abstract boolean isClosed();

    public abstract FileManager getFileManager();

    public abstract TimeMaster getTimeMaster();
    
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
    /* API, data reads
    /**********************************************************************
     */

    /**
     * Method that can be called to quickly see if there is an entry
     * for given key at this point. Note that soft deletions leave
     * "tombstones", so soft-deleted entries may return true from this method.
     */
    public abstract boolean hasEntry(StorableKey key) throws StoreException;

    /**
     * Accessor for getting entry for given key; this includes soft-deleted
     * entries ("tombstones") that have not yet been hard deleted (which typically
     * is done with some delay).
     */
    public abstract Storable findEntry(StorableKey key) throws StoreException;

    /*
    /**********************************************************************
    /* API, entry creation
    /**********************************************************************
     */
    
    /**
     * Method for inserting entry, <b>if and only if</b> no entry exists for
     * given key.
     * 
     * @param input Input stream used for reading the content. NOTE: method never
     *   closes this stream
     */
    public abstract StorableCreationResult insert(StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata)
        throws IOException, StoreException;

    /**
     * Method for inserting entry, if no entry exists for the key, or updating
     * entry if one does. In case of update, results will contain information
     * about overwritten entry.
     * 
     * @param input Input stream used for reading the content. NOTE: method never
     *   closes this stream
     * @param removeOldDataFile Whether method should delete backing data file for
     *   the existing entry (if one was found) or not.
     */
    public abstract StorableCreationResult upsert(StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile)
        throws IOException, StoreException;
    
    /*
    /**********************************************************************
    /* API, entry deletion
    /**********************************************************************
     */

    /**
     * Operation that will try to mark given entry as "tombstone", to indicate
     * logical deletion, but without deleting entry metadata. This is often
     * necessary to ensure that deletes are properly propagated, similar to how
     * insertions are.
     * 
     * @param key Key of entry to soft delete
     * @param removeInlinedData Whether operation should remove inlined data,
     *   if entry has any
     * @param removeExternalData Whether operation should remove external data,
     *   if entry has any.
     *
     * @return Status of the operation, including state of entry <b>after</b> changes
     *    specified have been made.
     */
    public abstract StorableDeletionResult softDelete(StorableKey key,
            final boolean removeInlinedData, final boolean removeExternalData)
        throws IOException, StoreException;
    
    /**
     * Operation that will try to physical delete entry matching given key.
     * It may also remove related external data, depending on arguments.
     * 
     * @param key Key of entry to soft delete
     * @param removeExternalData Whether operation should remove external data,
     *   if entry has any.
     *
     * @return Status of the operation, including state of entry <b>after</b> changes
     *    specified have been made.
     */
    public abstract StorableDeletionResult hardDelete(StorableKey key,
            final boolean removeExternalData)
        throws IOException, StoreException;

    /*
    /**********************************************************************
    /* API, iteration
    /**********************************************************************
     */

    /**
     * Method for iterating over entries store has,
     * in key order,
     * starting with specified key (inclusive).
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
    public abstract boolean iterateEntriesByKey(StorableIterationCallback cb,
            StorableKey firstKey)
        throws StoreException;

    /**
     * Method for iterating over entries store has,
     * in key order,
     * starting with specified key (inclusive).
     *<p>
     * Note that iteration is not transactional, in that operations
     * may modify entries during iteration process.
     * 
     * @param cb Callback used for actual iteration
     * @param firstTimestamp (optional) Last-modified timestamp of the first entry
     *   to include (inclusive).
     *   
     * @return True if iteration completed successfully; false if it was terminated
     */
    public abstract boolean iterateEntriesByModifiedTime(StorableIterationCallback cb,
            long firstTimestamp)
        throws StoreException;
}
