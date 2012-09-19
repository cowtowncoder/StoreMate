package com.fasterxml.storemate.backend.bdbje;

import java.io.File;
import java.io.IOException;

import com.sleepycat.je.*;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.WithBytesCallback;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.IterationResult;
import com.fasterxml.storemate.store.backend.StorableIterationCallback;
import com.fasterxml.storemate.store.backend.StorableLastModIterationCallback;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.impl.StorableConverter;

/**
 * {@link StoreBackend} implementation that builds on BDB-JE.
 * Note that per-entry locking is assumed to be provided by
 * caller; no attempt is made to synchronize individual operations
 * at store level.
 */
public class BDBJEStoreBackend extends StoreBackend
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
    
    public BDBJEStoreBackend(StorableConverter conv,
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

    @Override
    public long getEntryCount() {
        return _entries.count();
    }

    @Override
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
        OperationStatus status = _entries.get(null, dbKey(key), new DatabaseEntry(), null);
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
        OperationStatus status = _entries.get(null, dbKey(key), result, null);
        if (status != OperationStatus.SUCCESS) {
            return null;
        }
        return _storableConverter.decode(key, result.getData());
    }
    
    /*
    /**********************************************************************
    /* API Impl, insert/update
    /**********************************************************************
     */

    @Override
    public Storable createEntry(StorableKey key, Storable storable)
        throws IOException, StoreException
    {
        DatabaseEntry dbKey = dbKey(key);
        // first, try creating:
        OperationStatus status = _entries.putNoOverwrite(null, dbKey, dbValue(storable));
        if (status == OperationStatus.SUCCESS) { // the usual case:
            return null;
        }
        if (status != OperationStatus.KEYEXIST) { // what?
            throw new StoreException.Internal(key, "Internal error, strange return value for 'putNoOverwrite()': "+status);
        }
        // otherwise, ought to find existing entry, return it
        DatabaseEntry result = new DatabaseEntry();
        status = _entries.get(null, dbKey, result, null);
        if (status != OperationStatus.SUCCESS) { // sanity check, should never occur:
            throw new StoreException.Internal(key, "Internal error, failed to access old value, status: "+status);
        }
        return _storableConverter.decode(key, result.getData());
    }

    @Override
    public Storable putEntry(StorableKey key, Storable storable)
        throws IOException, StoreException
    {
        DatabaseEntry dbKey = dbKey(key);
        DatabaseEntry result = new DatabaseEntry();
        // First: do we have an entry? If so, read to be returned
        OperationStatus status = _entries.get(null, dbKey, result, null);
        if (status != OperationStatus.SUCCESS) {
            result = null;
        }
        // if not, create
        status = _entries.put(null, dbKey, dbValue(storable));
        if (status != OperationStatus.SUCCESS) {
            throw new StoreException.Internal(key, "Failed to put entry, OperationStatus="+status);
        }
        if (result == null) {
            return null;
        }
        return _storableConverter.decode(key, result.getData());
    }

    @Override
    public void ovewriteEntry(StorableKey key, Storable storable)
        throws IOException, StoreException
    {
        OperationStatus status = _entries.put(null, dbKey(key), dbValue(storable));
        if (status != OperationStatus.SUCCESS) {
            throw new StoreException.Internal(key, "Failed to overwrite entry, OperationStatus="+status);
        }
    }
    
    /*
    /**********************************************************************
    /* API Impl, delete
    /**********************************************************************
     */

    @Override
    public boolean deleteEntry(StorableKey key)
        throws IOException, StoreException
    {
        OperationStatus status = _entries.delete(null, dbKey(key));
        switch (status) {
        case SUCCESS:
            return true;
        case NOTFOUND:
            return false;
        default:
            // should not be getting other choices so:
            throw new StoreException.Internal(key, "Internal error, failed to delete entry, OperationStatus="+status);
        }
    }

    /*
    /**********************************************************************
    /* API Impl, iteration
    /**********************************************************************
     */

    @Override
    public IterationResult scanEntries(StorableIterationCallback cb)
        throws StoreException
    {
        DiskOrderedCursorConfig config = new DiskOrderedCursorConfig();
        DiskOrderedCursor crsr = _entries.openCursor(config);

        final DatabaseEntry keyEntry = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        
        try {
            main_loop:
            while (crsr.getNext(keyEntry, data, null) == OperationStatus.SUCCESS) {
                StorableKey key = storableKey(keyEntry);
                switch (cb.verifyKey(key)) {
                case SKIP_ENTRY: // nothing to do
                    continue main_loop;
                case PROCESS_ENTRY: // bind, process
                    break;
                case TERMINATE_ITERATION: // all done?
                    return IterationResult.TERMINATED_FOR_KEY;
                }
                Storable entry = _storableConverter.decode(key, data.getData());
                if (cb.processEntry(entry) == IterationAction.TERMINATE_ITERATION) {
                    return IterationResult.TERMINATED_FOR_ENTRY;
                }
            }
            return IterationResult.FULLY_ITERATED;
        } finally {
            crsr.close();
        }
    }

    @Override
    public IterationResult iterateEntriesByKey(StorableIterationCallback cb,
            StorableKey firstKey)
        throws StoreException
    {
        CursorConfig config = new CursorConfig();
        Cursor crsr = _entries.openCursor(null, config);
        final DatabaseEntry keyEntry;
        final DatabaseEntry data = new DatabaseEntry();

        OperationStatus status;
        if (firstKey == null) { // from beginning (i.e. no ranges)
            keyEntry = new DatabaseEntry();
            status = crsr.getFirst(keyEntry, data, null);
        } else {
            keyEntry = dbKey(firstKey);
            status = crsr.getSearchKeyRange(keyEntry, data, null);
        }

        try {
            main_loop:
            while (status == OperationStatus.SUCCESS) {
                StorableKey key = storableKey(keyEntry);
                switch (cb.verifyKey(key)) {
                case SKIP_ENTRY: // nothing to do
                    continue main_loop;
                case PROCESS_ENTRY: // bind, process
                    break;
                case TERMINATE_ITERATION: // all done?
                    return IterationResult.TERMINATED_FOR_KEY;
                }
                Storable entry = _storableConverter.decode(key, data.getData());
                if (cb.processEntry(entry) == IterationAction.TERMINATE_ITERATION) {
                    return IterationResult.TERMINATED_FOR_ENTRY;
                }
                status = crsr.getNext(keyEntry, data, null);
            }
            return IterationResult.FULLY_ITERATED;
        } finally {
            crsr.close();
        }
    }

    @Override
    public IterationResult iterateEntriesByModifiedTime(StorableLastModIterationCallback cb,
            long firstTimestamp)
        throws StoreException
    {
        if (cb == null) {
            throw new IllegalArgumentException("Can not pass null 'cb' argument");
        }
        
        CursorConfig config = new CursorConfig();
        SecondaryCursor crsr = _index.openCursor(null, config);
        final DatabaseEntry keyEntry;
        final DatabaseEntry primaryKeyEntry = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        
        OperationStatus status;
        if (firstTimestamp <= 0L) { // from beginning (i.e. no ranges)
            keyEntry = new DatabaseEntry();
            status = crsr.getFirst(keyEntry, primaryKeyEntry, data, null);
        } else {
            keyEntry = timestampKey(firstTimestamp);
            status = crsr.getSearchKeyRange(keyEntry, primaryKeyEntry, data, null);
        }
        
        try {
            main_loop:
            while (status == OperationStatus.SUCCESS) {
                // First things first: timestamp check
                long timestamp = _getLongBE(keyEntry.getData(), 0);
                switch (cb.verifyTimestamp(timestamp)) {
                case SKIP_ENTRY:
                    continue main_loop;
                case PROCESS_ENTRY:
                    break;
                case TERMINATE_ITERATION: // all done?
                    return IterationResult.TERMINATED_FOR_TIMESTAMP;
                }
                
                StorableKey key = storableKey(primaryKeyEntry);
                switch (cb.verifyKey(key)) {
                case SKIP_ENTRY: // nothing to do
                    continue main_loop;
                case PROCESS_ENTRY: // bind, process
                    break;
                case TERMINATE_ITERATION: // all done?
                    return IterationResult.TERMINATED_FOR_KEY;
                }
                Storable entry = _storableConverter.decode(key, data.getData());
                if (cb.processEntry(entry) == IterationAction.TERMINATE_ITERATION) {
                    return IterationResult.TERMINATED_FOR_ENTRY;
                }
                status = crsr.getNext(keyEntry, primaryKeyEntry, data, null);
            }
            return IterationResult.FULLY_ITERATED;
        } finally {
            crsr.close();
        }
    }
   
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    protected DatabaseEntry dbKey(StorableKey key) {
        return key.with(BDB_CONV);
    }

    protected DatabaseEntry dbValue(Storable storable) {
        return storable.withRaw(BDB_CONV);
    }
    
    protected StorableKey storableKey(DatabaseEntry entry) {
        return new StorableKey(entry.getData());
    }

    protected DatabaseEntry timestampKey(long timestamp)
    {
        byte[] raw = new byte[8];
        _putIntBE(raw, 0, (int) (timestamp >> 32));
        _putIntBE(raw, 4, (int) timestamp);
        return new DatabaseEntry(raw);
    }

    private final static void _putIntBE(byte[] buffer, int offset, int value)
    {
        buffer[offset] = (byte) (value >> 24);
        buffer[++offset] = (byte) (value >> 16);
        buffer[++offset] = (byte) (value >> 8);
        buffer[++offset] = (byte) value;
    }

    private final static long _getLongBE(byte[] buffer, int offset)
    {
        long l1 = _getIntBE(buffer, offset);
        long l2 = _getIntBE(buffer, offset+4);
        return (l1 << 32) | ((l2 << 32) >>> 32);
    }
    
    private final static int _getIntBE(byte[] buffer, int offset)
    {
        return (buffer[offset] << 24)
            | ((buffer[++offset] & 0xFF) << 16)
            | ((buffer[++offset] & 0xFF) << 8)
            | (buffer[++offset] & 0xFF)
            ;
    }
    
    /*
    /**********************************************************************
    /* Helper classes
    /**********************************************************************
     */
    
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
