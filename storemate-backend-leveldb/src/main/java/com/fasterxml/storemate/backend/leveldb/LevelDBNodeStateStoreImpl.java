package com.fasterxml.storemate.backend.leveldb;

import java.io.IOException;
import java.util.*;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.util.RawEntryConverter;
import com.fasterxml.storemate.store.state.NodeStateStore;

public class LevelDBNodeStateStoreImpl<K,V> extends NodeStateStore<K,V>
{
    private final Logger LOG;
    
    /**
     * Underlying LevelDB table
     * for storing node states.
     */
    protected final DB _store;

    public LevelDBNodeStateStoreImpl(Logger logger,
            RawEntryConverter<K> keyConv,
            RawEntryConverter<V> valueConv,
            DB store)
    {
        super(keyConv, valueConv);
        if (logger == null) {
            logger = LoggerFactory.getLogger(getClass());
        }
        LOG = logger;
        _store = store;
    }

    /*
    /**********************************************************************
    /* StartAndStoppable overrides
    /**********************************************************************
     */

    @Override
    public void start() { }
    @Override
    public void prepareForStop() {
        /* 27-Mar-2013, tatu: Not much we can do; sync() only needed when
         *   using deferred writes.
         */
//        _store.sync();
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
    /* Abstract method implementations
    /**********************************************************************
     */

    @Override
    public List<V> readAll() throws IOException
    {
        List<V> all = new ArrayList<V>(30);
        DBIterator iter = _store.iterator();
        try {
            int index = 0;
            while (iter.hasNext()) {
                Map.Entry<byte[], byte[]> entry = iter.next();
                ++index;
                try {
                    byte[] b = entry.getValue();
                    all.add(rawToValue(b, 0, b.length));
                } catch (Exception e) {
                    String keyStr = "N/A";
                    try {
                        byte[] rawKey = entry.getKey();
                        K key = rawToKey(rawKey, 0, rawKey.length);
                        keyStr = key.toString();
                    } catch (Exception e2) {
                        keyStr = "[Corrupt Key]";
                    }
                    LOG.error("Invalid Node state entry in {}, entry #{}, key '{}', skipping. Problem ({}): {}",
                            new Object[] { getClass(), index, keyStr, e.getClass().getName(), e.getMessage()});
                }
                ++index;
            }
        } finally {
            iter.close();
        }
        return all;
    }

    @Override
    protected V _findEntry(byte[] rawKey) throws IOException
    {
        byte[] rawData = _store.get(rawKey);
        if (rawData == null) {
            return null;
        }
        return rawToValue(rawData, 0, rawData.length);
    }

    @Override
    protected void _upsertEntry(byte[] rawKey, byte[] rawValue)
        throws IOException
    {
        _store.put(rawKey, rawValue);
    }

    @Override
    protected boolean _deleteEntry(byte[] rawKey) throws IOException
    {
        _store.delete(rawKey);
        // fairly meaningless, but...
        return true;
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
}
