package com.fasterxml.storemate.backend.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.iq80.leveldb.*;

import com.fasterxml.storemate.shared.StorableKey;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.*;
import com.fasterxml.storemate.store.impl.StorableConverter;
import com.fasterxml.storemate.store.util.OverwriteChecker;

/**
 * {@link StoreBackend} implementation that uses Java LevelDB implementation.
 * Note that per-entry locking is assumed to be provided by
 * caller; no attempt is made to synchronize individual operations
 * at store level.
 */
public class LevelDBStoreBackend extends StoreBackend
{
    private final static byte[] NO_BYTES = new byte[0];

    /*
    /**********************************************************************
    /* Simple config, location
    /**********************************************************************
     */

    protected final File _dataRoot;
    
    /*
    /**********************************************************************
    /* LevelDB entities
    /**********************************************************************
     */

    protected final DB _dataDB;

    protected final DB _indexDB;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public LevelDBStoreBackend(StorableConverter conv,
            File dbRoot, DB dataDB, DB indexDB)
    {
        super(conv);
        _dataRoot = dbRoot;
        _dataDB = dataDB;
        _indexDB = indexDB;
    }

    @Override
    public void start() {
        // nothing to do, yet
    }

    @Override
    public void prepareForStop() {
        // anything we can do? Could stop compactions but...
    }
    
    @Override
    public void stop()
    {
        try {
            _dataDB.close();
        } catch (IOException e) { }
        try {
            _indexDB.close();
        } catch (IOException e) { }
    }

    /*
    /**********************************************************************
    /* Capability introspection
    /**********************************************************************
     */

    /**
     * No, LevelDB does not have means to produce efficient entry count.
     */
    @Override
    public boolean hasEfficientEntryCount() { return false; }

    /**
     * No, LevelDB does not have means to produce efficient index entry count.
     */
    @Override
    public boolean hasEfficientIndexCount() { return false; }

    @Override
    public LevelDBBackendStats getEntryStatistics(BackendStatsConfig config) {
        return _getStats(_dataDB, config);
    }

    @Override
    public LevelDBBackendStats getIndexStatistics(BackendStatsConfig config) {
        return _getStats(_indexDB, config);
    }

    protected LevelDBBackendStats _getStats(DB db, BackendStatsConfig config)
    {
        Map<String,Object> stats = new LinkedHashMap<String,Object>();
        // JNI-version apparently exposes this; not sure about Java version:
        final String JNI_STATS = "leveldb.stats";
        String value = db.getProperty(JNI_STATS);
        if (value != null) {
            stats.put(JNI_STATS, value);
        }
        return new LevelDBBackendStats(config, System.currentTimeMillis(), stats);
    }
    
    /*
    /**********************************************************************
    /* API Impl, metadata
    /**********************************************************************
     */

    @Override
    public long getEntryCount() {
        return -1L;
    }

    @Override
    public long getIndexedCount() {
        return -1L;
    }

    @Override
    public long countEntries() throws StoreException {
        return _count(_dataDB);
    }

    @Override
    public long countIndexed() throws StoreException {
        return _count(_indexDB);
    }

    @Override
    public File getStorageDirectory() {
        return _dataRoot;
    }
    
    private final long _count(DB db) throws StoreException
    {
        long count = 0L;
        try {
            DBIterator iter = db.iterator();
            try {
                iter.seekToFirst();
                while (iter.hasNext()) {
                    ++count;
                    iter.next();
                }
                return count;
            } finally {
                try {
                    iter.close();
                } catch (IOException de) {
                    LevelDBUtil.convertIOE(null, de);
                }
            }
        } catch (DBException de) {
            LevelDBUtil.convertDBE(null, de);
        }
        return count; // never gets here
    }
    
    /*
    /**********************************************************************
    /* API Impl, read
    /**********************************************************************
     */

    @Override
    public boolean hasEntry(StorableKey key) throws StoreException
    {
        // Bah. No efficient method for this...
        return findEntry(key) != null;
    }
        
    @Override
    public Storable findEntry(StorableKey key) throws StoreException
    {
        try {
            byte[] data = _dataDB.get(dbKey(key)); // default options fine
            if (data == null) {
                return null;
            }
            return _storableConverter.decode(key, data);
        } catch (DBException de) {
            return LevelDBUtil.convertDBE(key, de);
        }
    }

    /*
    /**********************************************************************
    /* API Impl, insert/update
    /**********************************************************************
     */

    /* NOTE: all modification methods are protected by per-key partitioned
     * lock; so modification methods are transaction wrt other modifications,
     * although not wrt read methods. This has ramifications on ordering of
     * data vs index modifications.
     */
    
    @Override
    public Storable createEntry(StorableKey key, Storable newEntry)
        throws IOException, StoreException
    {
        byte[] dbKey = dbKey(key);
        try {
            // First things first: must check to see if an old entry exists; if so, return:
            byte[] oldData = _dataDB.get(dbKey);
            if (oldData != null) {
                return _storableConverter.decode(key, oldData);
            }
            // If not, insert entry, add index
            _dataDB.put(dbKey, newEntry.asBytes());
            _indexDB.put(keyToLastModEntry(dbKey, newEntry), NO_BYTES);
        } catch (DBException de) {
            return LevelDBUtil.convertDBE(key, de);
        }
        return null;
    }

    @Override
    public Storable putEntry(StorableKey key, Storable newEntry)
        throws IOException, StoreException
    {
        byte[] dbKey = dbKey(key);
        try {
            // First things first: must check to see if an old entry exists
            byte[] oldData = _dataDB.get(dbKey);
            Storable oldEntry = (oldData == null) ? null : _storableConverter.decode(key, oldData);
            // and if so, there's also index entry to remove, first
            if (oldEntry != null) {
                _indexDB.delete(keyToLastModEntry(dbKey, oldEntry));
            }
            // but then to actual business; insert new entry, index
            _dataDB.put(dbKey, newEntry.asBytes());
            // and index:
            _indexDB.put(keyToLastModEntry(dbKey, newEntry), NO_BYTES);
            return oldEntry;
        } catch (DBException de) {
            return LevelDBUtil.convertDBE(key, de);
        }
    }

    @Override
    public void ovewriteEntry(StorableKey key, Storable newEntry)
        throws IOException, StoreException
    {
        byte[] dbKey = dbKey(key);
        try {
            // Must check if an entry exists even if we don't return it, to
            // manage secondary index
            byte[] oldData = _dataDB.get(dbKey);
            if (oldData != null) {
                _indexDB.delete(keyToLastModEntry(dbKey, oldData));
            }
            _dataDB.put(dbKey, newEntry.asBytes());
            _indexDB.put(keyToLastModEntry(dbKey, newEntry), NO_BYTES);
        } catch (DBException de) {
            LevelDBUtil.convertDBE(key, de);
        }
    }

    @Override
    public boolean upsertEntry(StorableKey key, Storable newEntry,
            OverwriteChecker checker, AtomicReference<Storable> oldEntryRef)
        throws IOException, StoreException
    {
        byte[] dbKey = dbKey(key);
        try {
            byte[] oldData = _dataDB.get(dbKey);
            if (oldData != null) {
                Storable oldEntry = _storableConverter.decode(key, oldData);
                // yes: is it ok to overwrite?
                if (oldEntryRef != null) {
                    oldEntryRef.set(oldEntry);
                }
                if (!checker.mayOverwrite(key, oldEntry, newEntry)) {
                    // no, return
                    return false;
                }
                // but need to delete index
                _indexDB.delete(keyToLastModEntry(dbKey, oldEntry));
            } else {
                if (oldEntryRef != null) {
                    oldEntryRef.set(null);
                }
            }
            // Ok we are good, go ahead:
            _dataDB.put(dbKey, newEntry.asBytes());
            _indexDB.put(keyToLastModEntry(dbKey, newEntry), NO_BYTES);
            return true;
        } catch (DBException de) {
             LevelDBUtil.convertDBE(key, de);
             return false;
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
            /* Ok: we must actually fetch the entry, first, since we must have
             * lastmod timestamp to also delete index entry...
             */
            final byte[] dbKey = dbKey(key);
            byte[] data = _dataDB.get(dbKey);
            
            // No entry?
            if (data == null) {
                return false;
            }
            Storable value = _storableConverter.decode(key, data);
            // First remove index entry so we won't have dangling entries
            _indexDB.delete(keyToLastModEntry(dbKey, value));
            // can only return Snapshot, if we wanted that... 
            _dataDB.delete(dbKey);
            return true;
        } catch (DBException de) {
            LevelDBUtil.convertDBE(key, de);
            return false;
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
        // !!! TODO: make more efficient. Until then, just use in-order traversal
        //   Would Snapshot make sense here?
        return iterateEntriesByKey(cb, null);
    }

    @Override
    public IterationResult iterateEntriesByKey(StorableIterationCallback cb,
            StorableKey firstKey)
        throws StoreException
    {
        StorableKey key = null;
        try {
            DBIterator iter = _dataDB.iterator();
            try {
                if (firstKey == null) {
                    iter.seekToFirst();
                } else {
                    iter.seek(dbKey(firstKey));
                }
                main_loop:
                while (iter.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = iter.next();
                    key = storableKey(entry.getKey());
                    switch (cb.verifyKey(key)) {
                    case SKIP_ENTRY: // nothing to do
                        continue main_loop;
                    case PROCESS_ENTRY: // bind, process
                        break;
                    case TERMINATE_ITERATION: // all done?
                        return IterationResult.TERMINATED_FOR_KEY;
                    }
                    Storable dbValue = _storableConverter.decode(key, entry.getValue());
                    if (cb.processEntry(dbValue) == IterationAction.TERMINATE_ITERATION) {
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
    
    @Override
    public IterationResult iterateEntriesAfterKey(StorableIterationCallback cb,
            StorableKey lastSeen)
        throws StoreException
    {
        StorableKey key = null;
        try {
            final byte[] lastSeenRaw = dbKey(lastSeen);
            DBIterator iter = _dataDB.iterator();
            try {
                iter.seek(lastSeenRaw);
                // First: if we are at end, we are done
                if (!iter.hasNext()) { // last entry
                    return IterationResult.FULLY_ITERATED;
                }
                Map.Entry<byte[], byte[]> entry = iter.next();
                // First, did we find the entry (should, but better safe than sorry)
                byte[] b = entry.getKey();
                if (_equals(lastSeenRaw, b)) { // yes, same thingy -- skip
                    if (!iter.hasNext()) {
                        return IterationResult.FULLY_ITERATED;
                    }
                    entry = iter.next();
                    b = entry.getKey();
                }
                main_loop:
                while (true) {
                    key = storableKey(b);
                    switch (cb.verifyKey(key)) {
                    case TERMINATE_ITERATION: // all done?
                        return IterationResult.TERMINATED_FOR_KEY;
                    case PROCESS_ENTRY: // bind, process
                        break;
                    case SKIP_ENTRY: // nothing to do
                    default: // should we warn?
                        continue main_loop;
                    }
                    Storable dbEntry = _storableConverter.decode(key, entry.getValue());
                    if (cb.processEntry(dbEntry) == IterationAction.TERMINATE_ITERATION) {
                        return IterationResult.TERMINATED_FOR_ENTRY;
                    }
                    if (!iter.hasNext()) {
                        break;
                    }
                    entry = iter.next();
                    b = entry.getKey();
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
            DBIterator iter = _indexDB.iterator();

            if (firstTimestamp <= 0L) { // from beginning (i.e. no ranges)
                iter.seekToFirst();
            } else {
                iter.seek(timestampKey(firstTimestamp));
            }

            try {
                main_loop:
                while (iter.hasNext()) {
                    // First things first: timestamp check
                    byte[] rawKey = iter.next().getKey();
                    long timestamp = timestamp(rawKey);
                    switch (cb.verifyTimestamp(timestamp)) {
                    case SKIP_ENTRY:
                        continue main_loop;
                    case PROCESS_ENTRY:
                        break;
                    case TERMINATE_ITERATION: // all done?
                        return IterationResult.TERMINATED_FOR_TIMESTAMP;
                    }

                    key = _extractPrimaryKey(rawKey);
                    switch (cb.verifyKey(key)) {
                    case SKIP_ENTRY: // nothing to do
                        continue main_loop;
                    case PROCESS_ENTRY: // bind, process
                        break;
                    case TERMINATE_ITERATION: // all done?
                        return IterationResult.TERMINATED_FOR_KEY;
                    }
                    // and then find it...
                    byte[] rawEntry = _dataDB.get(dbKey(key));
                    // unusual but possible due to race condition:
                    IterationAction act;
                    if (rawEntry == null) {
                        act = cb.processMissingEntry(key);
                    } else {
                        // but more commonly:
                        act = cb.processEntry(_storableConverter.decode(key, rawEntry));
                    }
                    if (act == IterationAction.TERMINATE_ITERATION) {
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

    protected byte[] dbKey(StorableKey key) {
        return key.asBytes();
    }

    protected StorableKey storableKey(byte[] raw) {
        return new StorableKey(raw);
    }

    protected long timestamp(byte[] value) {
        return _getLongBE(value, 0);
    }

    protected byte[] timestampKey(long timestamp)
    {
        byte[] raw = new byte[8];
        _putLongBE(raw, 0, timestamp);
        return raw;
    }
    
    protected byte[] keyToLastModEntry(byte[] key, Storable value)
    {
        byte[] result = new byte[8 + key.length];
        long timestamp = value.getLastModified();
        _putLongBE(result, 0, timestamp);
        System.arraycopy(key, 0, result, 8, key.length);
        return result;
    }

    protected byte[] keyToLastModEntry(byte[] key, byte[] rawStorable)
    {
        byte[] result = new byte[8 + key.length];
        // Storable starts with timestamp, so just copy
        appendTimestamp(rawStorable, result, 0);
        System.arraycopy(key, 0, result, 8, key.length);
        return result;
    }
    
    protected int appendTimestamp(byte[] from, byte[] to, int toIndex)
    {
        to[toIndex] = from[0];
        to[++toIndex] = from[1];
        to[++toIndex] = from[2];
        to[++toIndex] = from[3];
        to[++toIndex] = from[4];
        to[++toIndex] = from[5];
        to[++toIndex] = from[6];
        to[++toIndex] = from[7];
        return toIndex+1;
    }

    protected StorableKey _extractPrimaryKey(byte[] indexKey) {
        // First 8 bytes are timestamp, rest is key
        return new StorableKey(indexKey, 8, indexKey.length - 8);
    }

    private final static void _putLongBE(byte[] buffer, int offset, long value)
    {
        _putIntBE(buffer, offset, (int) (value >> 32));
        _putIntBE(buffer, offset+4, (int) value);
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

    private final boolean _equals(byte[] b1, byte[] b2)
    {
        final int len = b1.length;
        if (b2.length != len) {
            return false;
        }
        for (int i = 0; i < len; ++i) {
            if (b1[i] != b2[i]) {
                return false;
            }
        }
        return true;
    }
}
