package com.fasterxml.storemate.backend.bdbje;

import java.util.concurrent.atomic.AtomicBoolean;

import com.sleepycat.je.*;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.backend.*;
import com.fasterxml.storemate.store.lastaccess.EntryLastAccessed;
import com.fasterxml.storemate.store.lastaccess.LastAccessConfig;
import com.fasterxml.storemate.store.lastaccess.LastAccessConverter;
import com.fasterxml.storemate.store.lastaccess.LastAccessStore;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;
import com.fasterxml.storemate.backend.bdbje.BDBBackendStats;
import com.fasterxml.storemate.backend.bdbje.util.BDBConverters;

/**
 * Intermediate base class for BDB-JE - backed {@link LastAccessStore}
 * implementation.
 */
public class BDBLastAccessStoreImpl<
        K extends StorableKey.Convertible,
        E extends StorableKey.Provider,
        ACC extends LastAccessUpdateMethod
>
    extends LastAccessStore<K,E,ACC>
{
    protected final LastAccessConverter<K,E,ACC> _lastAccessedConverter;

    /*
    /**********************************************************************
    /* BDB store for last-accessed timestamps
    /**********************************************************************
     */

    /**
     * Underlying BDB entity store ("table") for last-accessed timestamps
     */
    protected final Database _store;

    protected final AtomicBoolean _closed = new AtomicBoolean(true);
    
    /*
    /**********************************************************************
    /* Life cycle
    /**********************************************************************
     */

    public BDBLastAccessStoreImpl(LastAccessConfig config,
            LastAccessConverter<K, E, ACC> lastAccessedConverter,
            Environment env
//StoredEntryConverter<K,E,?> conv,
            )
        throws DatabaseException
    {
        super();
        _lastAccessedConverter = lastAccessedConverter;
        _store = env.openDatabase(null, // no TX
                "LastAccessed", dbConfig(env, config));
    }
    
    @Override
    public void start() {
        _closed.set(false);
    }
    
    @Override
    public void prepareForStop()
    {
        // mark this as closed already...  to help avoid cleanup task start
        _closed.set(true);
        
        // 02-May-2013, tsaloranta: Better sync() if we use deferred writes
        //   (otherwise not allowed to)
        if (_store.getConfig().getDeferredWrite()) {
            _store.sync();
        }
    }
    
    @Override
    public void stop() {
        _store.close();
    }

    /*
    /**********************************************************************
    /* Public API, metadata
    /**********************************************************************
     */

    @Override
    public boolean isClosed() {
        return _closed.get();
    }
    
    @Override
    public boolean hasEfficientEntryCount() {
        // yes, BDB-JE does have this info
        return true;
    }

    @Override
    public long getEntryCount() {
        return _store.count();
    }

    @Override
    public BackendStats getEntryStatistics(BackendStatsConfig config)
    {
        StatsConfig statsConfig = new StatsConfig()
            .setFast(config.onlyCollectFast())
            .setClear(config.resetStatsAfterCollection())
            ;
        final long start = System.currentTimeMillis();
        BDBBackendStats stats = new BDBBackendStats(config, start);
        stats.db = _store.getStats(statsConfig);
        stats.env = _store.getEnvironment().getStats(statsConfig);
        // don't accept "0 msecs" as answer :)
        stats.setTimeTakenMsecs(Math.max(1L, System.currentTimeMillis() - start));
        return stats;
    }

    /*
    /**********************************************************************
    /* Public API, lookups
    /**********************************************************************
     */

    @Override
    public long findLastAccessTime(E entry) {
        return _lastAccessedConverter.findLastAccessedTime(entry);
    }

    @Override
    public EntryLastAccessed findLastAccessEntry(K key, ACC method)
    {
        DatabaseEntry lastAccessKey = _lastAccessKey(key, method);
        if (lastAccessKey == null) {
            return null;
        }
        DatabaseEntry entry = new DatabaseEntry();
        OperationStatus status = _store.get(null, lastAccessKey, entry, null);
        switch (status) {
        case SUCCESS:
        case KEYEXIST:
            return _lastAccessedConverter.createLastAccessed(entry.getData(),
                    entry.getOffset(), entry.getSize());
        case KEYEMPTY: // was deleted during operation.. shouldn't be getting
        case NOTFOUND:
            // fall through
        }
        return null;
    }

    /*
    /**********************************************************************
    /* Public API, modifications
    /**********************************************************************
     */

    @Override
    public void updateLastAccess(E entry, long timestamp)
    {
        DatabaseEntry lastAccessKey = _lastAccessKey(entry);
        if (lastAccessKey != null) {
            /* 18-Sep-2012, tatu: Should we try to enforce constraint on monotonically
             *   increasing timestamps? Since this is not used for peer-to-peer syncing,
             *   minor deviations from exact value are ok (deletion occurs after hours,
             *   or at most minutes since last access), so let's avoid extra lookup.
             *   Same goes for other settings
             */
            EntryLastAccessed acc = _lastAccessedConverter.createLastAccessed(entry, timestamp);
                    //_entryConverter.createLastAccessed(entry, timestamp);
            _store.put(null, lastAccessKey, new DatabaseEntry(acc.asBytes()));
        }
    }

    @Override
    public boolean removeLastAccess(K key, ACC method, long timestamp) {
        return _remove(_lastAccessKey(key, method));
    }

    @Override
    public boolean removeLastAccess(StorableKey rawKey) {
        return _remove(BDBConverters.dbKey(rawKey));
    }
    
    protected boolean _remove(DatabaseEntry rawKey)
    {
        if (rawKey != null) {
            OperationStatus status = _store.delete(null, rawKey);
            switch (status) {
            case SUCCESS:
            case KEYEXIST:
                return true;
            default:
            }
        }
        return false;
    }

    /*
    /**********************************************************************
    /* Public API, iteration
    /**********************************************************************
     */

    @Override
    public IterationResult scanEntries(LastAccessIterationCallback cb)
        throws StoreException
    {
        try {
            DiskOrderedCursorConfig config = new DiskOrderedCursorConfig();
            DiskOrderedCursor crsr = _store.openCursor(config);
    
            final DatabaseEntry keyEntry = new DatabaseEntry();
            final DatabaseEntry data = new DatabaseEntry();
            
            try {
                while (crsr.getNext(keyEntry, data, null) == OperationStatus.SUCCESS) {
                    StorableKey key = _storableKey(keyEntry);
                    EntryLastAccessed entry = _lastAccessedConverter.createLastAccessed(data.getData(),
                            data.getOffset(), data.getSize());
                    if (cb.processEntry(key, entry) == IterationAction.TERMINATE_ITERATION) {
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
    
    protected StorableKey _storableKey(DatabaseEntry entry) {
        return new StorableKey(entry.getData(), entry.getOffset(), entry.getSize());
    }

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

    protected DatabaseConfig dbConfig(Environment env, LastAccessConfig config)
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        EnvironmentConfig econfig = env.getConfig();
        dbConfig.setReadOnly(econfig.getReadOnly());
        dbConfig.setAllowCreate(econfig.getAllowCreate());
        dbConfig.setSortedDuplicates(false);
        dbConfig.setDeferredWrite(config.useDeferredWrites());
        return dbConfig;
    }

    protected DatabaseEntry _lastAccessKey(E entry) {
        byte[] raw = _lastAccessedConverter.createLastAccessedKey(entry);
        return new DatabaseEntry(raw, 0, raw.length);
    }

    protected DatabaseEntry _lastAccessKey(K key, ACC method) {
        byte[] raw = _lastAccessedConverter.createLastAccessedKey(key, method);
        return new DatabaseEntry(raw, 0, raw.length);
    }
}
