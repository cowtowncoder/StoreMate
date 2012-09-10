package com.fasterxml.storemate.store;

import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;
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
    public abstract Storable createEntry(StorableKey key,
            StorableCreationMetadata stdMetadata, Storable storable)
        throws IOException, StoreException;

    /**
     * Method that inserts given entry in the database, possibly replacing
     * an existing version; also returns the old entry.
     * 
     * @return Existing entry, if any
     */
    public abstract Storable putEntry(StorableKey key,
            StorableCreationMetadata stdMetadata, Storable storable)
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
     * @throws IOExcetion If deletion failed due to underlying I/O problem, exposed by
     *   store (not all stores expose them)
     */
    public abstract boolean deleteEntry(StorableKey key)
        throws IOException, StoreException;
}
