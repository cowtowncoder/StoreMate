package com.fasterxml.storemate.store;

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

    /*
    /**********************************************************************
    /* API Impl, delete
    /**********************************************************************
     */
    
}
