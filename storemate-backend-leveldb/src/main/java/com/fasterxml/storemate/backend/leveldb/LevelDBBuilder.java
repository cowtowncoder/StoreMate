package com.fasterxml.storemate.backend.leveldb;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import com.fasterxml.storemate.store.StoreConfig;
import com.fasterxml.storemate.store.backend.StoreBackendBuilder;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;
import com.fasterxml.storemate.store.impl.StorableConverter;

public class LevelDBBuilder extends StoreBackendBuilder<LevelDBConfig>
{
    /**
     * For LevelDB we actually need two separate 'tables'; one for data,
     * another for last-modified index. Hence sub-directories.
     */
    public final static String DATA_DIR = "entries";
    
    public final static String LAST_MOD_DIR = "lastmod";
    
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
        _verifyDir(dbRoot, canCreate);
        File dataDir = new File(dbRoot, DATA_DIR);
        _verifyDir(dataDir, canCreate);
        File lastModDir = new File(dbRoot, LAST_MOD_DIR);
        _verifyDir(lastModDir, canCreate);
        
        StorableConverter storableConv = _storeConfig.createStorableConverter();

        Iq80DBFactory factory = Iq80DBFactory.factory;
        Options options = new Options();
        options = options
                .createIfMissing(canCreate)
                .logger(new LdbLogger())
                .verifyChecksums(false)
                ;
        
        DB dataDB;
        try {
            options = options.cacheSize(_levelDBConfig.dataCacheSize.getNumberOfBytes());
            dataDB = factory.open(dataDir, options);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open main data LevelDB: "+e.getMessage(), e);
        }
        DB indexDB;
        try {
            options = options.cacheSize(_levelDBConfig.dataCacheSize.getNumberOfBytes());
            indexDB = factory.open(lastModDir, options);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open last-mod index LevelDB: "+e.getMessage(), e);
        }
        return new LevelDBStoreBackend(storableConv, dbRoot, dataDB, indexDB);
    }

    protected void _verifyDir(File dir, boolean canCreate)
    {
        if (!dir.exists() || !dir.isDirectory()) {
            if (!canCreate) {
                throw new IllegalArgumentException("Directory '"+dir.getAbsolutePath()+"' does not exist, not allowed to (try to) create");
            }
            if (!dir.mkdirs()) {
                throw new IllegalArgumentException("Directory '"+dir.getAbsolutePath()+"' did not exist: failed to create it");
            }
        }
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
            throw new IllegalArgumentException("LevelDB must be configured with a LevelDBConfig instance, not "
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

    /*
    /**********************************************************************
    /* Helper types
    /**********************************************************************
     */

    static class LdbLogger implements Logger
    {
        protected final org.slf4j.Logger _slf4Logger;

        public LdbLogger()
        {
            _slf4Logger = org.slf4j.LoggerFactory.getLogger(LevelDBBuilder.class);
        }
        
        @Override
        public void log(String msg) {
            _slf4Logger.warn(msg);
        }
    }
}
