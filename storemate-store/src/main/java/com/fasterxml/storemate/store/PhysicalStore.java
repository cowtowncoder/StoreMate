package com.fasterxml.storemate.store;

import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Abstraction used by {@link StorableStore} for interacting with the
 * underlying physical database.
 */
public abstract class PhysicalStore
{
    protected final StorableConverter _storableConverter;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    protected PhysicalStore(StorableConverter storableConverter)
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
    /* API Impl, insert/update
    /**********************************************************************
     */

    public StorableCreationResult putEntry(StorableKey key,
            StorableCreationMetadata stdMetadata, Storable storable,
            boolean allowOverwrite)
        throws IOException, StoreException
    {
        if (allowOverwrite) { // "upsert"
            Storable old = putEntry(key, stdMetadata, storable);
            return new StorableCreationResult(key, true, old);
        }
        // strict "insert"
        Storable old = putEntry(key, stdMetadata, storable);
        if (old == null) { // ok, succeeded
            return new StorableCreationResult(key, true, null);
        }
        // fail: caller may need to clean up the underlying file
        return new StorableCreationResult(key, false, old);
    }
    
    /**
     * Method that tries to create specified entry in the database,
     * if (and only if!) no entry exists for given key.
     * If an entry exists, it will be returned and no changes are made.
     * 
     * @return Null if creation succeeded; or existing entry if not
     */
    protected abstract Storable createEntry(StorableKey key,
            StorableCreationMetadata stdMetadata, Storable storable)
        throws IOException, StoreException;

    /**
     * Method that inserts given entry in the database, possibly replacing
     * an existing version; also returns the old entry.
     * 
     * @return Existing entry, if any
     */
    protected abstract Storable putEntry(StorableKey key,
            StorableCreationMetadata stdMetadata, Storable storable)
        throws IOException, StoreException;

    /*
    /**********************************************************************
    /* API Impl, delete
    /**********************************************************************
     */
    
}
