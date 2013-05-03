package com.fasterxml.storemate.backend.bdbje;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.je.*;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.util.WithBytesCallback;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.*;
import com.fasterxml.storemate.store.impl.StorableConverter;
import com.fasterxml.storemate.store.util.OverwriteChecker;

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
            File dbRoot, Database entryDB, SecondaryDatabase lastModIndex)
    {
        super(conv);
        _dataRoot = dbRoot;
        _entries = entryDB;
        _index = lastModIndex;
    }

    @Override
    public void start() {
        // nothing to do, yet
    }

    @Override
    public void prepareForStop()
    {
        // If using deferred writes, better do sync() at this point
        if (_entries.getConfig().getDeferredWrite()) {
            _entries.sync();
        }
        if (_index.getConfig().getDeferredWrite()) {
            _index.sync();
        }
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
    /* Capability, statistics introspection
    /**********************************************************************
     */

    /**
     * Yes, BDB-JE can produce efficient entry count.
     */
    public boolean hasEfficientEntryCount() { return true; }

    /**
     * Yes, BDB-JE can produce efficient index entry count.
     */
    public boolean hasEfficientIndexCount() { return true; }

    @Override
    public DatabaseStats getEntryStatistics(BackendStatsConfig config) {
        return _getStats(_entries, config);
    }

    @Override
    public DatabaseStats getIndexStatistics(BackendStatsConfig config) {
        return _getStats(_entries, config);
    }

    protected DatabaseStats _getStats(Database db, BackendStatsConfig config) {
        StatsConfig statsConfig = new StatsConfig()
            .setFast(config.onlyCollectFast())
            .setClear(config.resetStatsAfterCollection())
            ;
        return db.getStats(statsConfig);
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

    @Override
    public long countEntries() throws StoreException
    {
	long count = 0L;
        try {
            DiskOrderedCursorConfig config = new DiskOrderedCursorConfig();
            DiskOrderedCursor crsr = _entries.openCursor(config);
    
            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();
            try {
                while (crsr.getNext(keyEntry, data, null) == OperationStatus.SUCCESS) {
                    ++count;
                }
                return count;
            } finally {
                crsr.close();
            }
        } catch (DatabaseException de) {
            _convertDBE(null, de);
	    return count;
        }
    }

    @Override
    public long countIndexed() throws StoreException
    {
	long count = 0L;
        try {
            SecondaryCursor crsr = _index.openCursor(null, new CursorConfig());
            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry primaryKeyEntry = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();

            try {
                OperationStatus status = crsr.getFirst(keyEntry, primaryKeyEntry, data, null);
                for (; status == OperationStatus.SUCCESS; status = crsr.getNext(keyEntry, primaryKeyEntry, data, null)) {
                    ++count;
                }
                return count;
            } finally {
                crsr.close();
            }
        } catch (DatabaseException de) {
            _convertDBE(null, de);
	    return count;
        }
    }

    /*
    /**********************************************************************
    /* API Impl, read
    /**********************************************************************
     */

    @Override
    public boolean hasEntry(StorableKey key) throws StoreException
    {
        try {
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
        } catch (DatabaseException de) {
            _convertDBE(key, de);
            return false; // stupid javac; some versions can coerce, others not
        }
    }
        
    @Override
    public Storable findEntry(StorableKey key) throws StoreException
    {
        DatabaseEntry result = new DatabaseEntry();
        try {
            OperationStatus status = _entries.get(null, dbKey(key), result, null);
            if (status != OperationStatus.SUCCESS) {
                return null;
            }
            return _storableConverter.decode(key, result.getData(), result.getOffset(), result.getSize());
        } catch (DatabaseException de) {
            return _convertDBE(key, de);
        }
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

        try {
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
            return _storableConverter.decode(key, result.getData(), result.getOffset(), result.getSize());
        } catch (DatabaseException de) {
            return _convertDBE(key, de);
        }
    }

    @Override
    public Storable putEntry(StorableKey key, Storable storable)
        throws IOException, StoreException
    {
        try {
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
            return _storableConverter.decode(key, result.getData(), result.getOffset(), result.getSize());
        } catch (DatabaseException de) {
            return _convertDBE(key, de);
        }
    }

    @Override
    public void ovewriteEntry(StorableKey key, Storable storable)
        throws IOException, StoreException
    {
        try {
            OperationStatus status = _entries.put(null, dbKey(key), dbValue(storable));
            if (status != OperationStatus.SUCCESS) {
                throw new StoreException.Internal(key, "Failed to overwrite entry, OperationStatus="+status);
            }
        } catch (DatabaseException de) {
            _convertDBE(key, de);
        }
    }

    @Override
    public boolean upsertEntry(StorableKey key, Storable storable,
            OverwriteChecker checker, AtomicReference<Storable> oldEntryRef)
        throws IOException, StoreException
    {
        try {
            DatabaseEntry dbKey = dbKey(key);
            DatabaseEntry result = new DatabaseEntry();
            // First: do we have an entry?
            OperationStatus status = _entries.get(null, dbKey, result, null);
            if (status == OperationStatus.SUCCESS) {
                // yes: is it ok to overwrite?
                Storable old = _storableConverter.decode(key, result.getData(), result.getOffset(), result.getSize());
                if (oldEntryRef != null) {
                    oldEntryRef.set(old);
                }
                if (!checker.mayOverwrite(key, old, storable)) {
                    // no, return
                    return false;
                }
            } else {
                if (oldEntryRef != null) {
                    oldEntryRef.set(null);
                }
            }
            // Ok we are good, go ahead:
            status = _entries.put(null, dbKey, dbValue(storable));
            if (status != OperationStatus.SUCCESS) {
                throw new StoreException.Internal(key, "Failed to put entry, OperationStatus="+status);
            }
            return true;
        } catch (DatabaseException de) {
            _convertDBE(key, de);
            return false; // stupid javac; some versions can coerce, others not
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
        try {
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
        } catch (DatabaseException de) {
            _convertDBE(key, de);
            return false; // stupid javac; some versions can coerce, others not
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
        try {
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
                    Storable entry = _storableConverter.decode(key, data.getData(), data.getOffset(), data.getSize());
                    if (cb.processEntry(entry) == IterationAction.TERMINATE_ITERATION) {
                        return IterationResult.TERMINATED_FOR_ENTRY;
                    }
                }
                return IterationResult.FULLY_ITERATED;
            } finally {
                crsr.close();
            }
        } catch (DatabaseException de) {
            return _convertDBE(null, de);
        }
    }

    @Override
    public IterationResult iterateEntriesByKey(StorableIterationCallback cb,
            StorableKey firstKey)
        throws StoreException
    {
        try {
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
                for (; status == OperationStatus.SUCCESS; status = crsr.getNext(keyEntry, data, null)) {
                    StorableKey key = storableKey(keyEntry);
                    switch (cb.verifyKey(key)) {
                    case SKIP_ENTRY: // nothing to do
                        continue main_loop;
                    case PROCESS_ENTRY: // bind, process
                        break;
                    case TERMINATE_ITERATION: // all done?
                        return IterationResult.TERMINATED_FOR_KEY;
                    }
                    Storable entry = _storableConverter.decode(key, data.getData(), data.getOffset(), data.getSize());
                    if (cb.processEntry(entry) == IterationAction.TERMINATE_ITERATION) {
                        return IterationResult.TERMINATED_FOR_ENTRY;
                    }
                    
                }
                return IterationResult.FULLY_ITERATED;
            } finally {
                crsr.close();
            }
        } catch (DatabaseException de) {
            return _convertDBE(null, de);
        }
    }

    @Override
    public IterationResult iterateEntriesAfterKey(StorableIterationCallback cb,
            StorableKey lastSeen)
        throws StoreException
    {
        try {
            Cursor crsr = _entries.openCursor(null, new CursorConfig());
    
            try {
                final DatabaseEntry data = new DatabaseEntry();
                final DatabaseEntry keyEntry = dbKey(lastSeen);
                OperationStatus status = crsr.getSearchKeyRange(keyEntry, data, null);
                do { // bogus loop so we can break
                    if (status != OperationStatus.SUCCESS) { // if it was the very last entry in store?
                        break;
                    }
                    // First, did we find the entry (should, but better safe than sorry)
                    byte[] b = keyEntry.getData();
                    if (lastSeen.equals(b, keyEntry.getOffset(), keyEntry.getSize())) { // yes, same thingy
                        status = crsr.getNext(keyEntry, data, null);
                        if (status != OperationStatus.SUCCESS) {
                            break;
                        }
                    }
                    main_loop:
                    for (; status == OperationStatus.SUCCESS; status = crsr.getNext(keyEntry, data, null)) {
                        StorableKey key = storableKey(keyEntry);
                        switch (cb.verifyKey(key)) {
                        case SKIP_ENTRY: // nothing to do
                            continue main_loop;
                        case PROCESS_ENTRY: // bind, process
                            break;
                        case TERMINATE_ITERATION: // all done?
                            return IterationResult.TERMINATED_FOR_KEY;
                        }
                        Storable entry = _storableConverter.decode(key, data.getData(), data.getOffset(), data.getSize());
                        if (cb.processEntry(entry) == IterationAction.TERMINATE_ITERATION) {
                            return IterationResult.TERMINATED_FOR_ENTRY;
                        }
                        
                    }
                } while (false);
                return IterationResult.FULLY_ITERATED;
            } finally {
                crsr.close();
            }
        } catch (DatabaseException de) {
            return _convertDBE(null, de);
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

        try {
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
                for (; status == OperationStatus.SUCCESS; status = crsr.getNext(keyEntry, primaryKeyEntry, data, null)) {
                    // First things first: timestamp check
                    long timestamp = _getLongBE(keyEntry.getData(), keyEntry.getOffset());
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
                    Storable entry = _storableConverter.decode(key, data.getData(), data.getOffset(), data.getSize());
                    if (cb.processEntry(entry) == IterationAction.TERMINATE_ITERATION) {
                        return IterationResult.TERMINATED_FOR_ENTRY;
                    }
                    
                }
                return IterationResult.FULLY_ITERATED;
            } finally {
                crsr.close();
            }
        } catch (DatabaseException de) {
            return _convertDBE(null, de);
        }
    }
   
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    /**
     * Helper method used for creating more useful exceptions for given BDB exception
     */
    protected <T> T _convertDBE(StorableKey key, DatabaseException bdbException)
        throws StoreException
    {
        if (bdbException instanceof LockTimeoutException) {
            throw new StoreException.ServerTimeout(key, bdbException);
        }
        throw new StoreException.Internal(key, bdbException);
    }
    
    protected DatabaseEntry dbKey(StorableKey key) {
        return key.with(BDB_CONV);
    }

    protected DatabaseEntry dbValue(Storable storable) {
        return storable.withRaw(BDB_CONV);
    }
    
    protected StorableKey storableKey(DatabaseEntry entry) {
        return new StorableKey(entry.getData(), entry.getOffset(), entry.getSize());
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
