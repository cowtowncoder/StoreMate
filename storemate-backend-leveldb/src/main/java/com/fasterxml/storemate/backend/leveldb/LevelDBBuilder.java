package com.fasterxml.storemate.backend.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.skife.config.DataAmount;

import com.fasterxml.storemate.store.StoreConfig;
import com.fasterxml.storemate.store.backend.StoreBackendBuilder;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;
import com.fasterxml.storemate.store.impl.StorableConverter;

public class LevelDBBuilder extends StoreBackendBuilder<LevelDBConfig>
{
    protected StoreConfig _storeConfig;
    protected LevelDBConfig _levelDBConfig;

    public LevelDBBuilder() { this(null, null); }

    public LevelDBBuilder(StoreConfig storeConfig, LevelDBConfig levelDBConfig)
    {
        super(LevelDBConfig.class);
        _storeConfig = storeConfig;
        _levelDBConfig = levelDBConfig;
    }

    @Override
    public LevelDBStoreBackend build() {
        return buildCreateAndInit();
    }

    /**
     * Method that will open an existing BDB database if one exists, or create
     * one if not, and create a store with that BDB. Underlying data storage
     * can do reads and writes.
     */
    public LevelDBStoreBackend buildCreateAndInit() {
        return _buildAndInit(true, true);
    }

    public LevelDBStoreBackend buildAndInitReadOnly() {
        return _buildAndInit(false, false);
    }

    public LevelDBStoreBackend buildAndInitReadWrite() {
        return _buildAndInit(false, true);
    }
    
    protected LevelDBStoreBackend _buildAndInit(boolean canCreate, boolean canWrite)
    {
        if (_storeConfig == null) throw new IllegalStateException("Missing StoreConfig");
        if (_levelDBConfig == null) throw new IllegalStateException("Missing LevelDBConfig");

        File dbRoot = _levelDBConfig.dataRoot;
        if (dbRoot == null) {
            throw new IllegalStateException("Missing LevelDBConfig.dataRoot");
        }
        if (!dbRoot.exists() || !dbRoot.isDirectory()) {
            if (!canCreate) {
                throw new IllegalArgumentException("Directory '"+dbRoot.getAbsolutePath()+"' does not exist, not allowed to (try to) create");
            }
            if (!dbRoot.mkdirs()) {
                throw new IllegalArgumentException("Directory '"+dbRoot.getAbsolutePath()+"' did not exist: failed to create it");
            }
        }
        StorableConverter storableConv = _storeConfig.createStorableConverter();

        /*
        Environment env = new Environment(dbRoot, envConfig(canCreate, canWrite));
        Database entryDB = env.openDatabase(null, // no TX
                "entryMetadata", dbConfig(env));
        SecondaryDatabase index = env.openSecondaryDatabase(null, "lastModIndex", entryDB,
                indexConfig(env));
        DataAmount cacheSize = _bdbConfig.cacheSize;
        BDBJEStoreBackend physicalStore = new BDBJEStoreBackend(storableConv,
                dbRoot, entryDB, index, cacheSize.getNumberOfBytes());

        try {
          physicalStore.start();
        } catch (DatabaseException e) {
            throw new IllegalStateException("Failed to open StorableStore: "+e.getMessage(), e);
        }
        return physicalStore;
        */

        Iq80DBFactory factory = Iq80DBFactory.factory;
        Options options = new Options();
        DB db;
        try {
            db = factory.open(dbRoot, options);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open StorableStore: "+e.getMessage(), e);
        }
        return new LevelDBStoreBackend(storableConv, dbRoot, db);
    }

    /*
    /**********************************************************************
    /* Fluent methods
    /**********************************************************************
     */
    
    @Override
    public LevelDBBuilder with(StoreConfig config) {
        _storeConfig = config;
        return this;
    }

    @Override
    public LevelDBBuilder with(StoreBackendConfig config) {
        if (!(config instanceof LevelDBConfig)) {
            String desc = (config == null) ? "NULL" : config.getClass().getName();
            throw new IllegalArgumentException("BDB-JE must be configured with a BDBJEConfig instance, not "
                    +desc);
        }
        _levelDBConfig = (LevelDBConfig) config;
        return this;
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
}
