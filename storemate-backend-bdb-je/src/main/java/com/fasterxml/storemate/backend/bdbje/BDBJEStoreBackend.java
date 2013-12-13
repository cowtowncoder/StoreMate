package com.fasterxml.storemate.backend.bdbje;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.je.*;

import com.fasterxml.storemate.backend.bdbje.util.LastModKeyCreator;
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

    protected final BDBJEConfig _bdbConfig;

    protected final EnvironmentConfig _envConfig;
    
    /*
    /**********************************************************************
    /* BDB entities
    /**********************************************************************
     */

    protected Environment _env;
    
    /**
     * Underlying primary BDB-JE database
     */
    protected Database _entries;

    /**
     * Secondary database that tracks last-modified order of primary entries.
     */
    protected SecondaryDatabase _index;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public BDBJEStoreBackend(StorableConverter conv, File dbRoot,
            BDBJEConfig bdbConfig, EnvironmentConfig envConfig)
        throws DatabaseException
    {
        super(conv);
        _dataRoot = dbRoot;
        _bdbConfig = bdbConfig;
        _envConfig = envConfig;

        openBDB(dbRoot, bdbConfig, envConfig);
    }
    
    private synchronized void openBDB(File dbRoot, BDBJEConfig bdbConfig, EnvironmentConfig envConfig)
        throws DatabaseException
    {
        _env = new Environment(_dataRoot, _envConfig);
        _entries = _env.openDatabase(null, // no TX
                "entryMetadata", dbConfig(_bdbConfig, _env));
        _index = _env.openSecondaryDatabase(null, "lastModIndex", _entries,
                indexConfig(_bdbConfig, _env));
    }

    @Override
    public void start() {
        // nothing to do, yet
    }

    @Override
    public void prepareForStop()
    {
        // If using deferred writes, better do sync() at this point
        if (_env.isValid()) {
            if (_entries.getConfig().getDeferredWrite()) {
                _entries.sync();
            }
            if (_index.getConfig().getDeferredWrite()) {
                _index.sync();
            }
        }
    }

    @Override
    public void stop()
    {
        /* 06-Sep-2013, tatu: There are cases where the whole Environment is
         *   busted; let's try to tone down error reporting for such cases.
         */
        if (_env.isValid()) {
            _index.close();
            _entries.close();
            _env.close();
        } else {
            LOG.warn("BDB-JE Environment not valid on stop(): will not try closing Database instances, only Environment itself");
            // If invalid, only close environment itself; db handles are invalid anyway
            _env.close();
        }
    }

    protected DatabaseConfig dbConfig(BDBJEConfig bdbConfig, Environment env)
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        EnvironmentConfig econfig = env.getConfig();
        dbConfig.setReadOnly(econfig.getReadOnly());
        dbConfig.setAllowCreate(econfig.getAllowCreate());
        dbConfig.setTransactional(bdbConfig.useTransactions);
        dbConfig.setSortedDuplicates(false);
        // we can opt to use deferred writes if we dare:
        dbConfig.setDeferredWrite(bdbConfig.useDeferredWritesForEntries());
        return dbConfig;
    }

    protected SecondaryConfig indexConfig(BDBJEConfig bdbConfig, Environment env)
    {
        LastModKeyCreator keyCreator = new LastModKeyCreator();
        SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setAllowCreate(env.getConfig().getAllowCreate());
        secConfig.setTransactional(bdbConfig.useTransactions);
        // should not need to auto-populate; except if re-creating broken
        // indexes? (in which case one would have to drop index first, then re-open)
        secConfig.setAllowPopulate(false);
        secConfig.setKeyCreator(keyCreator);
        // important: timestamps are not unique, need to allow dups:
        secConfig.setSortedDuplicates(true);
        // no, it is not immutable (entries will be updated with new timestamps)
        secConfig.setImmutableSecondaryKey(false);
        secConfig.setDeferredWrite(bdbConfig.useDeferredWritesForEntries());
        return secConfig;
    }
    
    /*
    /**********************************************************************
    /* Capability, statistics introspection
    /**********************************************************************
     */

    /**
     * Yes, BDB-JE can produce efficient entry count.
     */
    @Override
    public boolean hasEfficientEntryCount() { return true; }

    /**
     * Yes, BDB-JE can produce efficient index entry count.
     */
    @Override
    public boolean hasEfficientIndexCount() { return true; }

    @Override
    public BackendStats getEntryStatistics(BackendStatsConfig config) {
        return _getStats(_entries, config, true);
    }

    @Override
    public BackendStats getIndexStatistics(BackendStatsConfig config) {
        return _getStats(_entries, config, false);
    }

    @Override
    public File getStorageDirectory() {
        return _dataRoot;
    }
    
    protected BackendStats _getStats(Database db, BackendStatsConfig config,
            boolean includeEnvStats) {
        StatsConfig statsConfig = new StatsConfig()
            .setFast(config.onlyCollectFast())
            .setClear(config.resetStatsAfterCollection())
            ;
        /* 16-May-2013, tatu: Would be great to be able to remove/clear deprecated
         *   entries here... alas, no mutators, so need to leave them as is,
         *   for now.
         */
        // Should we require creationTime to be accessed via TimeMaster?
        BDBBackendStats stats = new BDBBackendStats(config, System.currentTimeMillis());
        final long start = System.currentTimeMillis();
        stats.db = db.getStats(statsConfig);
        if (includeEnvStats) {
            stats.env = db.getEnvironment().getStats(statsConfig);
        }
        // let's not accept "no time taken" as valid, always at least 1 msec:
        final long taken = System.currentTimeMillis() - start;
        stats.setTimeTakenMsecs(Math.max(1L, taken));
        return stats;
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
        StorableKey key = null;
        try {
            DiskOrderedCursorConfig config = new DiskOrderedCursorConfig();
            DiskOrderedCursor crsr = _entries.openCursor(config);
    
            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();
            
            try {
                OperationStatus status;
                status = crsr.getNext(keyEntry, data, null);

                while (status == OperationStatus.SUCCESS) {
                    key = storableKey(keyEntry);
                    switch (cb.verifyKey(key)) {
                    case TERMINATE_ITERATION: // all done?
                        return IterationResult.TERMINATED_FOR_KEY;
                    case PROCESS_ENTRY: // bind, process
                        Storable entry = _storableConverter.decode(key, data.getData(), data.getOffset(), data.getSize());
                        // IMPORTANT: need to advance cursor before calling process!
                        status = crsr.getNext(keyEntry, data, null);
                        if (cb.processEntry(entry) == IterationAction.TERMINATE_ITERATION) {
                            return IterationResult.TERMINATED_FOR_ENTRY;
                        }
                        break;
                    default: // SKIP_ENTRY
                        status = crsr.getNext(keyEntry, data, null);
                    }
                }
                return IterationResult.FULLY_ITERATED;
            } finally {
                crsr.close();
            }
        } catch (DatabaseException de) {
            return _convertDBE(key, de);
        }
    }

    @Override
    public IterationResult iterateEntriesByKey(StorableIterationCallback cb,
            StorableKey firstKey)
        throws StoreException
    {
        StorableKey key = null;
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
                while (status == OperationStatus.SUCCESS) {
                    key = storableKey(keyEntry);
                    switch (cb.verifyKey(key)) {
                    case TERMINATE_ITERATION:
                        return IterationResult.TERMINATED_FOR_KEY;
                    case PROCESS_ENTRY:
                        Storable entry = _storableConverter.decode(key, data.getData(), data.getOffset(), data.getSize());
                        // IMPORTANT: need to advance cursor before calling process!
                        status = crsr.getNext(keyEntry, data, null);
                        if (cb.processEntry(entry) == IterationAction.TERMINATE_ITERATION) {
                            return IterationResult.TERMINATED_FOR_ENTRY;
                        }
                        break;
                    default: // SKIP_ENTRY
                        status = crsr.getNext(keyEntry, data, null);
                    }
                }
                return IterationResult.FULLY_ITERATED;
            } finally {
                crsr.close();
            }
        } catch (DatabaseException de) {
            return _convertDBE(key, de);
        }
    }

    @Override
    public IterationResult iterateEntriesAfterKey(StorableIterationCallback cb,
            StorableKey lastSeen)
        throws StoreException
    {
        StorableKey key = null;
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
                    while (status == OperationStatus.SUCCESS) {
                        key = storableKey(keyEntry);
                        switch (cb.verifyKey(key)) {
                        case TERMINATE_ITERATION: // all done?
                            return IterationResult.TERMINATED_FOR_KEY;
                        case PROCESS_ENTRY: // bind, process
                            Storable entry = _storableConverter.decode(key, data.getData(), data.getOffset(), data.getSize());
                            // IMPORTANT: need to advance cursor before calling process!
                            status = crsr.getNext(keyEntry, data, null);
                            if (cb.processEntry(entry) == IterationAction.TERMINATE_ITERATION) {
                                return IterationResult.TERMINATED_FOR_ENTRY;
                            }
                            break;
                        default: // SKIP_ENTRY:
                            status = crsr.getNext(keyEntry, data, null);
                        }
                    }
                } while (false);
                return IterationResult.FULLY_ITERATED;
            } finally {
                crsr.close();
            }
        } catch (DatabaseException de) {
            return _convertDBE(key, de);
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
        StorableKey key = null;
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
                while (status == OperationStatus.SUCCESS) {
                    // First things first: timestamp check
                    long timestamp = _getLongBE(keyEntry.getData(), keyEntry.getOffset());
                    switch (cb.verifyTimestamp(timestamp)) {
                    case TERMINATE_ITERATION: // all done?
                        return IterationResult.TERMINATED_FOR_TIMESTAMP;
                    case PROCESS_ENTRY:
                        break;
                    default: // SKIP_ENTRY
                        status = crsr.getNext(keyEntry, primaryKeyEntry, data, null);
                        continue main_loop;
                    }
                    key = storableKey(primaryKeyEntry);
                    switch (cb.verifyKey(key)) {
                    case TERMINATE_ITERATION: // all done?
                        return IterationResult.TERMINATED_FOR_KEY;
                    case PROCESS_ENTRY: // bind, process
                        Storable entry = _storableConverter.decode(key, data.getData(), data.getOffset(), data.getSize());
                        status = crsr.getNext(keyEntry, primaryKeyEntry, data, null);
                        if (cb.processEntry(entry) == IterationAction.TERMINATE_ITERATION) {
                            return IterationResult.TERMINATED_FOR_ENTRY;
                        }
                        break;
                    default: // SKIP_ENTRY
                        status = crsr.getNext(keyEntry, primaryKeyEntry, data, null);
                    }
                }
                return IterationResult.FULLY_ITERATED;
            } finally {
                crsr.close();
            }
        } catch (DatabaseException de) {
            return _convertDBE(key, de);
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
        if (bdbException instanceof SecondaryIntegrityException) {
            throw new StoreException.DB(key, StoreException.DBProblem.SECONDARY_INDEX_CORRUPTION,
                    bdbException);
        }
        throw new StoreException.DB(key, StoreException.DBProblem.OTHER, bdbException);
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
