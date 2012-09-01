package com.fasterxml.storemate.store.bdb;

import java.io.File;
import java.io.IOException;

import com.sleepycat.je.*;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.WithBytesCallback;

import com.fasterxml.storemate.store.*;

/**
 * {@link PhysicalStore} implementation that builds on BDB-JE.
 * Note that per-entry locking is assumed to be provided by
 * caller; no attempt is made to synchronize individual operations
 * at store level.
 */
public class PhysicalBDBStore extends PhysicalStore
{
    private final BDBConverter BDB_CONV = new BDBConverter();
    
    /*
    /**********************************************************************
    /* Simple config, location
    /**********************************************************************
     */

    protected final File _dataRoot;

    /*
    /**********************************************************************
    /* BDB entities
    /**********************************************************************
     */

    /**
     * Underlying primary BDB-JE database
     */
    protected final Database _entries;

    /**
     * Secondary database that tracks last-modified order of primary entries.
     */
    protected final SecondaryDatabase _index;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public PhysicalBDBStore(StorableConverter conv,
            File dbRoot, Database entryDB, SecondaryDatabase lastModIndex,
            long bdbCacheSize)
    {
        super(conv);
        _dataRoot = dbRoot;
        _entries = entryDB;
        _index = lastModIndex;
    }

    @Override
    public void start()
    {
        // nothing to do, yet
    }
    
    @Override
    public void stop()
    {
        Environment env = _entries.getEnvironment();
        _index.close();
        _entries.close();
        env.close();
    }
    
    /*
    /**********************************************************************
    /* API Impl, metadata
    /**********************************************************************
     */

    public long getEntryCount() {
        return _entries.count();
    }

    /**
     * Accessor for getting approximate count of entries accessible
     * via last-modifed index.
     */
    public long getIndexedCount() {
        return _entries.count();
    }

    /*
    /**********************************************************************
    /* API Impl, read
    /**********************************************************************
     */

    @Override
    public boolean hasEntry(StorableKey key)
    {
        OperationStatus status = _entries.get(null, dbKey(key), null, LockMode.READ_COMMITTED);
        switch (status) {
        case SUCCESS:
        case KEYEXIST:
            return true;
        case KEYEMPTY: // was deleted during operation.. shouldn't be getting
        case NOTFOUND:
            // fall through
        }
        return false;
    }

    @Override
    public Storable findEntry(StorableKey key) throws StoreException
    {
        DatabaseEntry result = new DatabaseEntry();
        OperationStatus status = _entries.get(null, dbKey(key), result, LockMode.READ_COMMITTED);
        if (status != OperationStatus.SUCCESS) {
            return null;
        }
        return _storableConverter.decode(result.getData());
    }
    
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
    @Override
    public Storable createEntry(StorableKey key,
            StorableCreationMetadata stdMetadata, Storable storable)
        throws IOException, StoreException
    {
        DatabaseEntry dbKey = dbKey(key);
        // first, try creating:
        OperationStatus status = _entries.putNoOverwrite(null, dbKey, dbValue(storable));
        if (status == OperationStatus.SUCCESS) { // the usual case:
            return null;
        }
        if (status != OperationStatus.KEYEXIST) { // what?
            throw new StoreException(key, "Internal error, strange return value for 'putNoOverwrite()': "+status);
        }
        // otherwise, ought to find existing entry, return it
        DatabaseEntry result = new DatabaseEntry();
        status = _entries.get(null, dbKey, result, LockMode.READ_COMMITTED);
        if (status != OperationStatus.SUCCESS) { // sanity check, should never occur:
            throw new StoreException(key, "Internal error, failed to access old value, status: "+status);
        }
        return _storableConverter.decode(result.getData());
    }

    /**
     * Method that inserts given entry in the database, possibly replacing
     * an existing version; also returns the old entry.
     * 
     * @return Existing entry, if any
     */
    @Override
    public Storable putEntry(StorableKey key,
            StorableCreationMetadata stdMetadata, Storable storable)
        throws IOException, StoreException
    {
        DatabaseEntry dbKey = dbKey(key);
        DatabaseEntry result = new DatabaseEntry();
        // First: do we have an entry? If so, read to be returned
        OperationStatus status = _entries.get(null, dbKey, result, LockMode.READ_COMMITTED);
        if (status != OperationStatus.SUCCESS) {
            result = null;
        }
        // if not, create
        status = _entries.put(null, dbKey, dbValue(storable));
        if (status != OperationStatus.SUCCESS) {
            throw new StoreException(key, "Failed to PUT entry, response status: "+status);
        }
        if (result == null) {
            return null;
        }
        return _storableConverter.decode(result.getData());
    }

    /*
    /**********************************************************************
    /* API Impl, delete
    /**********************************************************************
     */

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    protected DatabaseEntry dbKey(StorableKey key)
    {
        return key.with(BDB_CONV);
    }

    protected DatabaseEntry dbValue(Storable storable)
    {
        return storable.withRaw(BDB_CONV);
    }
    
    private final static class BDBConverter implements WithBytesCallback<DatabaseEntry>
    {
        @Override
        public DatabaseEntry withBytes(byte[] buffer, int offset, int length) {
            if (offset == 0 && length == buffer.length) {
                return new DatabaseEntry(buffer);
            }
            return new DatabaseEntry(buffer, offset, length);
        }
    }
}
