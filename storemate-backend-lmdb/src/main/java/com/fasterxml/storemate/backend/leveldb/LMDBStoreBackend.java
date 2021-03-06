package com.fasterxml.storemate.backend.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.fusesource.lmdbjni.*;

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
public class LMDBStoreBackend extends StoreBackend
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

    protected final Env _env;

    protected final Database _dataDB;

    protected final Database _indexDB;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public LMDBStoreBackend(StorableConverter conv,
            File dbRoot, Env env, Database dataDB, Database indexDB)
    {
        super(conv);
        _dataRoot = dbRoot;
        _env = env;
        _dataDB = dataDB;
        _indexDB = indexDB;
    }

    @Override
    public void start() {
        // nothing to do, yet
    }

    @Override
    public void prepareForStop() {
        // anything we can do?
    }
    
    @Override
    public void stop()
    {
        // First, close databases
        try {
            _dataDB.close();
        } catch (/*LMDB*/Exception e) {
        }
        try {
            _indexDB.close();
        } catch (/*LMDB*/Exception e) {
        }
        // and then, very importantly, environment as well
        _env.close();
    }

    /*
    /**********************************************************************
    /* Capability introspection
    /**********************************************************************
     */

    /**
     * No, LevelDB does not have means to produce efficient entry count.
     */
    public boolean hasEfficientEntryCount() { return false; }

    /**
     * No, LevelDB does not have means to produce efficient index entry count.
     */
    public boolean hasEfficientIndexCount() { return false; }

    @Override
    public Map<String,Object> getEntryStatistics(BackendStatsConfig config) {
        return _getStats(_dataDB, config);
    }

    @Override
    public Map<String,Object> getIndexStatistics(BackendStatsConfig config) {
        return _getStats(_indexDB, config);
    }

    protected Map<String,Object> _getStats(Database db, BackendStatsConfig config)
    {
        Map<String,Object> stats = new LinkedHashMap<String,Object>();
        // actually of (private) type import org.fusesource.lmdbjni.JNI.MDB_stat
        // with public fields -- should be convertible with Jackson
        // or such... but here, expose as is:
        /*MDB_stat*/ Object rawStats = db.stat();
        if (rawStats != null) {
            stats.put("stats", rawStats);
        }
        return stats;
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

    public long countEntries() throws StoreException {
        return _count(_dataDB);
    }

    @Override
    public long countIndexed() throws StoreException {
        return _count(_indexDB);
    }

    private final long _count(Database db) throws StoreException
    {
        long count = 0L;
        try {
            Transaction tx = _env.createTransaction(true);
            Cursor iter = db.openCursor(tx);
            try {
                Entry entry = iter.get(GetOp.FIRST);
                while (entry != null) {
                    ++count;
                    entry = iter.get(GetOp.NEXT);
                }
                return count;
            } finally {
                try {
                    iter.close();
                } catch (LMDBException de) {
                    _convertIOE(null, de);
                }
                try {
                    tx.commit();
                } catch (LMDBException de) {
                    _convertIOE(null, de);
                }
            }
        } catch (LMDBException de) {
            _convertDBE(null, de);
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
        } catch (LMDBException de) {
            return _convertDBE(key, de);
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
     * data vs index mods.
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
        } catch (LMDBException de) {
            return _convertDBE(key, de);
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
        } catch (LMDBException de) {
            return _convertDBE(key, de);
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
        } catch (LMDBException de) {
            _convertDBE(key, de);
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
        } catch (LMDBException de) {
             _convertDBE(key, de);
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
        } catch (LMDBException de) {
            _convertDBE(key, de);
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
        /*
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
        } catch (LMDBException de) {
            return _convertDBE(null, de);
        }
        */
        return null;
    }

    @Override
    public IterationResult iterateEntriesByKey(StorableIterationCallback cb,
            StorableKey firstKey)
        throws StoreException
    {
        try {
            Transaction tx = _env.createTransaction(true);
            Cursor iter = _dataDB.openCursor(tx);
            try {
                Entry entry;
                if (firstKey == null) {
                    entry = iter.get(GetOp.FIRST);
                } else {
                    entry = iter.seek(SeekOp.RANGE, dbKey(firstKey));
                }
                main_loop:
                for (; entry != null; entry = iter.get(GetOp.NEXT)) {
                    StorableKey key = storableKey(entry.getKey());
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
                } catch (LMDBException de) {
                    return _convertIOE(null, de);
                }
                try {
                    tx.commit();
                } catch (LMDBException de) {
                    _convertIOE(null, de);
                }
            }
        } catch (LMDBException de) {
            return _convertDBE(null, de);
        }
    }
    
    @Override
    public IterationResult iterateEntriesAfterKey(StorableIterationCallback cb,
            StorableKey lastSeen)
        throws StoreException
    {
        try {
            final byte[] lastSeenRaw = dbKey(lastSeen);
            Transaction tx = _env.createTransaction(true);
            Cursor iter = _dataDB.openCursor(tx);
            try {
                Entry entry = iter.seek(SeekOp.RANGE, lastSeenRaw);
                // First: if we are at end, we are done
                if (entry == null) { // was last entry
                    return IterationResult.FULLY_ITERATED;
                }
                // First, did we find the entry (should, but better safe than sorry)
                byte[] b = entry.getKey();
                if (_equals(lastSeenRaw, b)) { // yes, same thingy -- skip
                    entry = iter.get(GetOp.NEXT);
                    if (entry == null) {
                        return IterationResult.FULLY_ITERATED;
                    }
                    b = entry.getKey();
                }
                main_loop:
                while (true) {
                    StorableKey key = storableKey(b);
                    switch (cb.verifyKey(key)) {
                    case SKIP_ENTRY: // nothing to do
                        continue main_loop;
                    case TERMINATE_ITERATION: // all done?
                        return IterationResult.TERMINATED_FOR_KEY;
                    case PROCESS_ENTRY: // bind, process
                    }
                    Storable dbEntry = _storableConverter.decode(key, entry.getValue());
                    if (cb.processEntry(dbEntry) == IterationAction.TERMINATE_ITERATION) {
                        return IterationResult.TERMINATED_FOR_ENTRY;
                    }
                    entry = iter.get(GetOp.NEXT);
                    if (entry == null) {
                        break;
                    }
                    b = entry.getKey();
                }
                return IterationResult.FULLY_ITERATED;
            } finally {
                try {
                    iter.close();
                } catch (LMDBException de) {
                    return _convertIOE(null, de);
                }
                try {
                    tx.commit();
                } catch (LMDBException de) {
                    _convertIOE(null, de);
                }
            }
        } catch (LMDBException de) {
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
            Transaction tx = _env.createTransaction(true);
            Cursor iter = _indexDB.openCursor(tx);

            Entry entry;
            if (firstTimestamp <= 0L) { // from beginning (i.e. no ranges)
                entry = iter.get(GetOp.FIRST);
            } else {
                entry = iter.seek(SeekOp.RANGE, timestampKey(firstTimestamp));
            }
            
            try {
                main_loop:
                for (; entry != null; entry = iter.get(GetOp.NEXT)) {
                    // First things first: timestamp check
                    byte[] rawKey = entry.getKey();
                    long timestamp = timestamp(rawKey);
                    switch (cb.verifyTimestamp(timestamp)) {
                    case SKIP_ENTRY:
                        continue main_loop;
                    case PROCESS_ENTRY:
                        break;
                    case TERMINATE_ITERATION: // all done?
                        return IterationResult.TERMINATED_FOR_TIMESTAMP;
                    }

                    StorableKey key = _extractPrimaryKey(rawKey);
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
                } catch (LMDBException de) {
                    return _convertIOE(null, de);
                }
            }
        } catch (LMDBException de) {
            return _convertDBE(null, de);
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
        _putIntBE(raw, 0, (int) (timestamp >> 32));
        _putIntBE(raw, 4, (int) timestamp);
        return raw;
    }
    
    protected byte[] keyToLastModEntry(byte[] key, Storable value)
    {
        byte[] result = new byte[key.length + 8];
        long ts = value.getLastModified();
        _putIntBE(result, 0, (int) (ts >> 32));
        _putIntBE(result, 4, (int) ts);
        System.arraycopy(key, 0, result, 8, key.length);
        return result;
    }

    protected byte[] keyToLastModEntry(byte[] key, byte[] rawStorable)
    {
        byte[] result = new byte[key.length + 8];
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
    
    protected <T> T _convertDBE(StorableKey key, LMDBException dbException)
        throws StoreException
    {
        // any special types that require special handling... ?
        /*
        if (bdbException instanceof LockTimeoutException) {
            throw new StoreException.ServerTimeout(key, bdbException);
        }
        */
        throw new StoreException.Internal(key, dbException);
    }

    protected <T> T _convertIOE(StorableKey key, LMDBException ioe)
            throws StoreException
        {
            // any special types that require special handling... ?
            throw new StoreException.Internal(key, ioe);
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
