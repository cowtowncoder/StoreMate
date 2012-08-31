package com.fasterxml.storemate.store.bdb;

import java.io.File;

import com.sleepycat.je.*;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.WithBytesCallback;

import com.fasterxml.storemate.store.PhysicalStore;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableConverter;
import com.fasterxml.storemate.store.StoreException;

public class PhysicalBDBStore extends PhysicalStore
{
    private final KeyConverter KEY_CONV = new KeyConverter();
    
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
        return key.with(KEY_CONV);
    }

    private final static class KeyConverter implements WithBytesCallback<DatabaseEntry>
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
