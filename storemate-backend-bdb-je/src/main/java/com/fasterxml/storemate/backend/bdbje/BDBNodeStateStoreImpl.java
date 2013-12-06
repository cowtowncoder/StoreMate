package com.fasterxml.storemate.backend.bdbje;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.util.RawEntryConverter;
import com.fasterxml.storemate.store.state.NodeStateStore;
import com.sleepycat.je.*;

public class BDBNodeStateStoreImpl<K,V> extends NodeStateStore<K,V>
{
    private final Logger LOG;
    
    /**
     * Underlying BDB entity store ("table")
     * for storing node states.
     */
    protected final Database _store;

    public BDBNodeStateStoreImpl(Logger logger,
            RawEntryConverter<K> keyConv,
            RawEntryConverter<V> valueConv,
            Environment env)
    {
        super(keyConv, valueConv);
        if (logger == null) {
            logger = LoggerFactory.getLogger(getClass());
        }
        LOG = logger;
        _store = env.openDatabase(null, // no TX, since we use atomic operations
                "Nodes", dbConfig(env));
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
        _store.close();
//        LOG.info("Closing Node store environment...");
        _store.getEnvironment().close();
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
        CursorConfig config = new CursorConfig();
        Cursor crsr = _store.openCursor(null, config);
        final DatabaseEntry keyEntry;
        final DatabaseEntry valueEntry = new DatabaseEntry();

        keyEntry = new DatabaseEntry();
        OperationStatus status = crsr.getFirst(keyEntry, valueEntry, null);
        try {
            int index = 0;

            for (; status == OperationStatus.SUCCESS; status = crsr.getNext(keyEntry, valueEntry, null)) {
                try {
                    all.add(dbToValue(valueEntry));
                } catch (Exception e) {
                    String keyStr = "N/A";
                    try {
                        K key = this.rawToKey(keyEntry.getData(), keyEntry.getOffset(), keyEntry.getSize());
                        keyStr = key.toString();
                    } catch (Exception e2) {
                        keyStr = "[Corrupt Key]";
                    }
                    LOG.error("Invalid Node state entry in BDB, entry #{}, key '{}', skipping. Problem ({}): {}",
                            new Object[] { index, keyStr, e.getClass().getName(), e.getMessage()});
                }
                ++index;
            }
        } finally {
            crsr.close();
        }
        return all;
    }

    @Override
    protected V _findEntry(byte[] rawKey) throws IOException
    {
        DatabaseEntry valueEntry = new DatabaseEntry();
        OperationStatus status = _store.get(null, dbEntry(rawKey), valueEntry, null);
        switch (status) {
        case SUCCESS:
        case KEYEXIST:
            return dbToValue(valueEntry);
        case KEYEMPTY: // was deleted during operation.. shouldn't be getting
        case NOTFOUND:
            // fall through
        }
        return null;
    }

    @Override
    protected void _upsertEntry(byte[] rawKey, byte[] rawValue)
        throws IOException
    {
        _store.put(null, dbEntry(rawKey), dbEntry(rawValue));
    }

    @Override
    protected boolean _deleteEntry(byte[] rawKey) throws IOException {
        OperationStatus status = _store.delete(null, dbEntry(rawKey));
        switch (status) {
        case SUCCESS:
        case KEYEXIST:
            return true;
        default:
        }
        return false;
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected DatabaseConfig dbConfig(Environment env)
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        EnvironmentConfig econfig = env.getConfig();
        dbConfig.setReadOnly(econfig.getReadOnly());
        dbConfig.setAllowCreate(econfig.getAllowCreate());
        dbConfig.setSortedDuplicates(false);
        return dbConfig;
    }

    protected DatabaseEntry dbEntry(byte[] rawKey) {
//        return new DatabaseEntry(UTF8Encoder.encodeAsUTF8(key));
        return new DatabaseEntry(rawKey);
    }

    protected V dbToValue(DatabaseEntry entry) throws IOException {
        return rawToValue(entry.getData(), entry.getOffset(), entry.getSize());
    }
    
    /*
    protected IpAndPort _keyFromDB(DatabaseEntry entry) {
        return new IpAndPort(UTF8Encoder.decodeFromUTF8(entry.getData(),
                entry.getOffset(), entry.getSize()));
    }
    */
}
