package com.fasterxml.storemate.backend.leveldb;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.backend.*;
import com.fasterxml.storemate.store.lastaccess.*;

/**
 * Intermediate base class for BDB-JE - backed {@link LastAccessStore}
 * implementation.
 */
public class LevelDBLastAccessStore<
        K extends StorableKey.Convertible,
        E extends StorableKey.Provider,
        ACC extends LastAccessUpdateMethod
>
    extends LastAccessStore<K,E,ACC>
{

    private final Logger LOG;
    
    protected final LastAccessConverter<K,E,ACC> _lastAccessedConverter;

    /*
    /**********************************************************************
    /* BDB store for last-accessed timestamps
    /**********************************************************************
     */

    /**
     * Underlying LevelDB table for storing node states.
     */
    protected final DB _store;

    protected final AtomicBoolean _closed = new AtomicBoolean(true);
    
    /*
    /**********************************************************************
    /* Life cycle
    /**********************************************************************
     */

    public LevelDBLastAccessStore(Logger logger,
            LastAccessConfig config,
            LastAccessConverter<K, E, ACC> lastAccessedConverter,
            DB store)
    {
        super();
        if (logger == null) {
            logger = LoggerFactory.getLogger(getClass());
        }
        LOG = logger;
        _lastAccessedConverter = lastAccessedConverter;
        _store = store;
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
    }
    
    @Override
    public void stop() {
        try {
            _store.close();
        } catch (IOException e) {
            LOG.warn("Problems closing {}: ({}) {}",
                    getClass(), e.getClass(), e.getMessage());
//        LOG.info("Closing Node store environment...");
//        _store.getEnvironment().close();
        }
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
        // no, not yet?
        return false;
    }

    @Override
    public long getEntryCount() {
        return -1L;
    }

    @Override
    public BackendStats getEntryStatistics(BackendStatsConfig config)
    {
        Map<String,Object> stats = new LinkedHashMap<String,Object>();
        // JNI-version apparently exposes this; not sure about Java version:
        final String JNI_STATS = "leveldb.stats";
        String value = _store.getProperty(JNI_STATS);
        if (value != null) {
            stats.put(JNI_STATS, value);
        }
        return new LevelDBBackendStats(config, System.currentTimeMillis(), stats);
    }

    /*
    /**********************************************************************
    /* Public API, lookups
    /**********************************************************************
     */

    @Override
    public EntryLastAccessed findLastAccessEntry(K key, ACC method) {
        return _findLastAccess(_lastAccessKey(key, method));
    }

    @Override
    public EntryLastAccessed findLastAccessEntry(E entry) {
        return _findLastAccess(_lastAccessKey(entry));
    }
    
    protected EntryLastAccessed _findLastAccess(byte[] lastAccessKey)
    {
        if (lastAccessKey == null) {
            return null;
        }
        byte[] raw = _store.get(lastAccessKey);
        if (raw != null) {
            return _lastAccessedConverter.createLastAccessed(raw, 0, raw.length);
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
        byte[] lastAccessKey = _lastAccessKey(entry);
        if (lastAccessKey != null) {
            /* 18-Sep-2012, tatu: Should we try to enforce constraint on monotonically
             *   increasing timestamps? Since this is not used for peer-to-peer syncing,
             *   minor deviations from exact value are ok (deletion occurs after hours,
             *   or at most minutes since last access), so let's avoid extra lookup.
             *   Same goes for other settings
             */
            EntryLastAccessed acc = _lastAccessedConverter.createLastAccessed(entry, timestamp);
                    //_entryConverter.createLastAccessed(entry, timestamp);
            _store.put(lastAccessKey, acc.asBytes());
        }
    }

    @Override
    public boolean removeLastAccess(K key, ACC method, long timestamp) {
        return _remove(_lastAccessKey(key, method));
    }

    @Override
    public boolean removeLastAccess(StorableKey rawKey) {
        return _remove(rawKey.asBytes());
    }
    
    protected boolean _remove(byte[] rawKey)
    {
        if (rawKey != null) {
            _store.delete(rawKey);
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
        // !!! TODO: make more efficient. Until then, just use in-order traversal
        //   Would Snapshot make sense here?
        StorableKey key = null;
        try {
            DBIterator iter = _store.iterator();
            try {
                iter.seekToFirst();

                while (iter.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = iter.next();
                    key = _storableKey(entry.getKey());
                    byte[] b = key.asBytes();
                    EntryLastAccessed lastAcc = _lastAccessedConverter.createLastAccessed(b, 0, b.length);
                    if (cb.processEntry(key, lastAcc) == IterationAction.TERMINATE_ITERATION) {
                        return IterationResult.TERMINATED_FOR_ENTRY;
                    }
                }
                return IterationResult.FULLY_ITERATED;
            } finally {
                try {
                    iter.close();
                } catch (IOException de) {
                    return LevelDBUtil.convertIOE(key, de);
                }
            }
        } catch (DBException de) {
            return LevelDBUtil.convertDBE(key, de);
        }
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    private byte[] _lastAccessKey(E entry) {
        return _lastAccessedConverter.createLastAccessedKey(entry);
    }

    private byte[] _lastAccessKey(K key, ACC method) {
        return _lastAccessedConverter.createLastAccessedKey(key, method);
    }

    protected StorableKey _storableKey(byte[] raw) {
        return new StorableKey(raw);
    }
}
