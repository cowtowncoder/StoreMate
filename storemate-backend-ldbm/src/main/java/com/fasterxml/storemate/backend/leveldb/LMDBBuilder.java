package com.fasterxml.storemate.backend.leveldb;

import java.io.File;

import org.fusesource.lmdbjni.*;

import com.fasterxml.storemate.store.StoreConfig;
import com.fasterxml.storemate.store.backend.StoreBackendBuilder;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;
import com.fasterxml.storemate.store.impl.StorableConverter;

public class LMDBBuilder extends StoreBackendBuilder<LMDBConfig>
{
    /**
     * For LevelDB we actually need two separate 'tables'; one for data,
     * another for last-modified index. Hence sub-directories.
     */
    public final static String NAME_DATA = "entries";
    
    public final static String NAME_LAST_MOD = "lastmod";
    
    protected StoreConfig _storeConfig;
    protected LMDBConfig _levelDBConfig;

    public LMDBBuilder() { this(null, null); }

    public LMDBBuilder(StoreConfig storeConfig, LMDBConfig levelDBConfig)
    {
        super(LMDBConfig.class);
        _storeConfig = storeConfig;
        _levelDBConfig = levelDBConfig;
    }

    @Override
    public LMDBStoreBackend build() {
        return buildCreateAndInit();
    }

    /**
     * Method that will open an existing BDB database if one exists, or create
     * one if not, and create a store with that BDB. Underlying data storage
     * can do reads and writes.
     */
    public LMDBStoreBackend buildCreateAndInit() {
        return _buildAndInit(true, true);
    }

    public LMDBStoreBackend buildAndInitReadOnly() {
        return _buildAndInit(false, false);
    }

    public LMDBStoreBackend buildAndInitReadWrite() {
        return _buildAndInit(false, true);
    }
    
    protected LMDBStoreBackend _buildAndInit(boolean canCreate,
            boolean canWrite)
    {
        if (_storeConfig == null) throw new IllegalStateException("Missing StoreConfig");
        if (_levelDBConfig == null) throw new IllegalStateException("Missing LevelDBConfig");

        File dbRoot = _levelDBConfig.dataRoot;
        if (dbRoot == null) {
            throw new IllegalStateException("Missing LevelDBConfig.dataRoot");
        }
        _verifyDir(dbRoot, canCreate);
        
        StorableConverter storableConv = _storeConfig.createStorableConverter();

        Env env = new Env();
        
        Database dataDB;
        int flags = canCreate ? Constants.CREATE : 0;
        if (!canWrite) {
            flags |= Constants.RDONLY;
        }
        try {
            dataDB = env.openDatabase(NAME_DATA, flags);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open main data LMDB: "+e.getMessage(), e);
        }
        Database indexDB;
        try {
            indexDB = env.openDatabase(NAME_LAST_MOD, flags);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open last-mod index LevelDB: "+e.getMessage(), e);
        }
        return new LMDBStoreBackend(storableConv, dbRoot, env,
                dataDB, indexDB);
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
    public LMDBBuilder with(StoreConfig config) {
        _storeConfig = config;
        return this;
    }

    @Override
    public LMDBBuilder with(StoreBackendConfig config) {
        if (!(config instanceof LMDBConfig)) {
            String desc = (config == null) ? "NULL" : config.getClass().getName();
            throw new IllegalArgumentException("BDB-JE must be configured with a BDBJEConfig instance, not "
                    +desc);
        }
        _levelDBConfig = (LMDBConfig) config;
        return this;
    }
}
