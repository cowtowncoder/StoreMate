package com.fasterxml.storemate.store;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.storemate.shared.*;

import com.fasterxml.storemate.store.backend.IterationResult;
import com.fasterxml.storemate.store.backend.StorableIterationCallback;
import com.fasterxml.storemate.store.backend.StorableLastModIterationCallback;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.util.OperationDiagnostics;
import com.fasterxml.storemate.store.util.OverwriteChecker;

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

    public abstract void start() throws Exception;

    /**
     * Method that is called before {@link #stop}, to give an advance warning
     * (if possible) about impending shutdown.
     * 
     * @since 0.9.7
     */
    public abstract void prepareForStop() throws Exception;

    public abstract void stop() throws Exception;
    
    /*
    /**********************************************************************
    /* API, simple accessors for state, helper objects
    /**********************************************************************
     */

    public abstract boolean isClosed();

    public abstract FileManager getFileManager();

    public abstract TimeMaster getTimeMaster();

    public abstract StoreBackend getBackend();

    public abstract StoreOperationThrottler getThrottler();
    
    /*
    /**********************************************************************
    /* API, store metadata access
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

    /**
     * Method that can be called to find if there are write operations in-flight,
     * and if so, find the oldest associated timestamp (minimum of all timestamps
     * to be used as last-modified values) and return it.
     * This can be used to calculate high-water marks for traversing last-modified
     * index (to avoid accessing things modified after start of traversal).
     * Note that this only establishes conservative lower bound: due to race condition,
     * the oldest operation may finish before this method returns.
     * 
     * @return Timestamp of the "oldest" write operation still being performed,
     *    if any, or 0L if none
     */
    public abstract long getOldestInFlightTimestamp();
    
    /*
    /**********************************************************************
    /* API, data reads
    /**********************************************************************
     */

    /**
     * Method that can be called to quickly see if there is an entry
     * for given key at this point. Note that soft deletions leave
     * "tombstones", so this methods may return for soft-deleted entries.
     */
    public abstract boolean hasEntry(StoreOperationSource source, OperationDiagnostics diag,
            StorableKey key)
        throws IOException, StoreException;

    /**
     * Accessor for getting entry for given key; this includes soft-deleted
     * entries ("tombstones") that have not yet been hard deleted (which typically
     * is done with some delay).
     */
    public abstract Storable findEntry(StoreOperationSource source, OperationDiagnostics diag,
            StorableKey key)
        throws IOException, StoreException;
    
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
    public abstract StorableCreationResult insert(StoreOperationSource source, StorableKey key,
            InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata)
        throws IOException, StoreException;

    /**
     * Method for inserting entry, <b>if and only if</b> no entry exists for
     * given key.
     * 
     * @param input Input data to store (usually inlined)
     */
    public abstract StorableCreationResult insert(StoreOperationSource source, StorableKey key,
            ByteContainer input,
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
    public abstract StorableCreationResult upsert(StoreOperationSource source, StorableKey key,
            InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile)
        throws IOException, StoreException;

    /**
     * Method for inserting entry, if no entry exists for the key, or updating
     * entry if one does. In case of update, results will contain information
     * about overwritten entry.
     * 
     * @param input Payload to store with entry
     * @param removeOldDataFile Whether method should delete backing data file for
     *   the existing entry (if one was found) or not.
     */
    public abstract StorableCreationResult upsert(StoreOperationSource source, StorableKey key,
            ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile)
        throws IOException, StoreException;

    /**
     * Method for inserting an entry, or possibly updating existing entry; latter depending
     * in result of a callback caller passes, which lets it determine whether update
     * should proceed.
     * 
     * @param input Payload to store with entry
     * @param removeOldDataFile Whether method should delete backing data file for
     *   the existing entry (if one was found) or not.
     * @param checker Object that is called if overwriting of content is necessary, to check whether
     *   update is allowed.
     *   
     * @since 0.9.3
     */
    public abstract StorableCreationResult upsertConditionally(StoreOperationSource source, StorableKey key,
            InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile, OverwriteChecker checker)
        throws IOException, StoreException;

    /**
     * Method for inserting an entry, or possibly updating existing entry; latter depending
     * in result of a callback caller passes, which lets it determine whether update
     * should proceed.
     * 
     * @param input Payload to store with entry
     * @param removeOldDataFile Whether method should delete backing data file for
     *   the existing entry (if one was found) or not.
     * @param checker Object that is called if overwriting of content is necessary, to check whether
     *   update is allowed.
     *
     * @since 0.9.3
     */
    public abstract StorableCreationResult upsertConditionally(StoreOperationSource source, StorableKey key,
            ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile, OverwriteChecker checker)
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
     * Note that <b>entry data</b> may be deleted, depending on arguments. 
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
    public abstract StorableDeletionResult softDelete(StoreOperationSource source, StorableKey key,
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
    public abstract StorableDeletionResult hardDelete(StoreOperationSource source, StorableKey key,
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
     * @return Value that indicates how iteration ended
     */
    public abstract IterationResult iterateEntriesByKey(StoreOperationSource source,
            StorableKey firstKey,
            StorableIterationCallback cb)
        throws StoreException;

    /**
     * Method for iterating over entries store has,
     * in key order,
     * starting with entry <b>AFTER</b> specified key.
     *<p>
     * Note that iteration is not transactional, in that operations
     * may modify entries during iteration process.
     * 
     * @param cb Callback used for actual iteration
     * @param firstKey (optional) If not null, key for the first entry
     *   to include (inclusive); if null, starts from the very first entry
     *
     * @return Value that indicates how iteration ended
     */
    public abstract IterationResult iterateEntriesAfterKey(StoreOperationSource source,
            StorableKey firstKey,
            StorableIterationCallback cb)
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
    public abstract IterationResult iterateEntriesByModifiedTime(StoreOperationSource source,
            long firstTimestamp,
            StorableLastModIterationCallback cb)
        throws StoreException;
}
