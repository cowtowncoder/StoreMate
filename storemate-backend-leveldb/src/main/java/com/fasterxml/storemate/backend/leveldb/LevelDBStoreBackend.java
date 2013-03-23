package com.fasterxml.storemate.backend.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.util.WithBytesCallback;

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
    public void start()
    {
        // nothing to do, yet
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
    public Storable createEntry(StorableKey key, Storable storable)
        throws IOException, StoreException
    {
        // NOTE: caller provides mutex for key (partitioned locks by key), hence transactional
        byte[] dbKey = dbKey(key);
        try {
            // First things first: must check to see if an old entry exists; if so, return:
            byte[] oldData = _dataDB.get(dbKey);
            if (oldData != null) {
                return _storableConverter.decode(key, oldData);
            }
            // Then entry
            _dataDB.put(dbKey, storable.asBytes());
            // and index:
            _indexDB.put(keyToLastModEntry(dbKey, storable), NO_BYTES);
        } catch (DBException de) {
            return _convertDBE(key, de);
        }
        return null;
    }

    @Override
    public Storable putEntry(StorableKey key, Storable storable)
        throws IOException, StoreException
    {
        /*
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
                */
        return null;

    }

    @Override
    public void ovewriteEntry(StorableKey key, Storable storable)
        throws IOException, StoreException
    {
        /*
        try {
            OperationStatus status = _entries.put(null, dbKey(key), dbValue(storable));
            if (status != OperationStatus.SUCCESS) {
                throw new StoreException.Internal(key, "Failed to overwrite entry, OperationStatus="+status);
            }
        } catch (DatabaseException de) {
            _convertDBE(key, de);
        }
        */
    }

    @Override
    public boolean upsertEntry(StorableKey key, Storable storable,
            OverwriteChecker checker, AtomicReference<Storable> oldEntryRef)
        throws IOException, StoreException
    {
        /*
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
            return _convertDBE(key, de);
        }
        */
        return false;
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
            return _convertDBE(key, de);
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
        } catch (DatabaseException de) {
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
        /*
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
        */
        return null;
    }

    @Override
    public IterationResult iterateEntriesAfterKey(StorableIterationCallback cb,
            StorableKey lastSeen)
        throws StoreException
    {
        /*
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
        */
        return null;
    }
    
    @Override
    public IterationResult iterateEntriesByModifiedTime(StorableLastModIterationCallback cb,
            long firstTimestamp)
        throws StoreException
    {
        if (cb == null) {
            throw new IllegalArgumentException("Can not pass null 'cb' argument");
        }

        /*
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
        */
        return null;
        
    }
   
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected byte[] dbKey(StorableKey key) {
        return key.asBytes();
    }

    protected long timestamp(byte[] value) {
        return _getLongBE(value, 0);
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
    
    protected <T> T _convertDBE(StorableKey key, DBException dbException)
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
    
}
