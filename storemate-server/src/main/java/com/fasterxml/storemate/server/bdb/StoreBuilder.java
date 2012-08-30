package com.fasterxml.storemate.server.bdb;

import java.io.File;

import com.sleepycat.je.*;

import com.fasterxml.storemate.server.file.FileManager;

/**
 * Helper object used for configuring and instantiating
 * {@link StorableStore} instances.
 */
public class StoreBuilder
{
    protected FileManager _fileManager;
    protected StoreConfig _config;

    protected StoreBuilder() { }
    public StoreBuilder(StoreConfig config, FileManager fileManager)
    {
        _config = config;
        _fileManager = fileManager;
    }

    /**
     * Method that will open an existing BDB database if one exists, or create
     * one if not, and create a store with that BDB. Underlying data storage
     * can do reads and writes.
     */
    public StorableStore buildCreateAndInit() {
        return _buildAndInit(true, true);
    }

    public StorableStore buildAndInitReadOnly() {
        return _buildAndInit(false, false);
    }

    public StorableStore buildAndInitReadWrite() {
        return _buildAndInit(false, true);
    }
    
    protected StorableStore _buildAndInit(boolean canCreate, boolean canWrite)
    {
        if (_config == null) throw new IllegalStateException("Missing StoreConfig");
        if (_fileManager == null) throw new IllegalStateException("Missing FileManager");

        StorableConverter storableConv = _config.createStorableConverter();
        
        File dbRoot = new File(_config.dataRootPath);
        if (!dbRoot.exists() || !dbRoot.isDirectory()) {
            if (!canCreate) {
                throw new IllegalArgumentException("Directory '"+dbRoot.getAbsolutePath()+"' does not exist, not allowed to (try to) create");
            }
            if (!dbRoot.mkdirs()) {
                throw new IllegalArgumentException("Directory '"+dbRoot.getAbsolutePath()+"' did not exist: failed to create it");
            }
        }
        Environment env = new Environment(dbRoot, envConfig(canCreate, canWrite));
        Database entryDB = env.openDatabase(null, // no TX
                "entryMetadata", dbConfig(env));
        SecondaryDatabase index = env.openSecondaryDatabase(null, "lastModIndex", entryDB,
                indexConfig(env));
        
        try {
            StorableStore store = new StorableStore(_config,
                    dbRoot, _fileManager, entryDB, index, storableConv);
            store.start();
            return store;
        } catch (DatabaseException e) {
            throw new IllegalStateException("Failed to open StorableStore: "+e.getMessage(), e);
        }
    }

    /*
    /**********************************************************************
    /* Fluent methods
    /**********************************************************************
     */
    
    public StoreBuilder with(StoreConfig config) {
        _config = config;
        return this;
    }

    public StoreBuilder with(FileManager mgr) {
        _fileManager = mgr;
        return this;
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    protected EnvironmentConfig envConfig(boolean allowCreate, boolean writeAccess)
    {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(allowCreate);
        config.setReadOnly(!writeAccess);
        config.setSharedCache(false);
        config.setCacheSize(_config.cacheInBytes);
        return config;
    }

    protected DatabaseConfig dbConfig(Environment env)
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        EnvironmentConfig econfig = env.getConfig();
        dbConfig.setReadOnly(econfig.getReadOnly());
        dbConfig.setAllowCreate(econfig.getAllowCreate());
        dbConfig.setSortedDuplicates(false);
        return dbConfig;
    }

    protected SecondaryConfig indexConfig(Environment env)
    {
        LastModKeyCreator keyCreator = new LastModKeyCreator();
        SecondaryConfig config = new SecondaryConfig();
        config.setAllowCreate(env.getConfig().getAllowCreate());
        // should not need to auto-populate ever:
        config.setAllowPopulate(false);
        config.setKeyCreator(keyCreator);
        // no, it is not immutable (entries will be updated with new timestamps)
        config.setImmutableSecondaryKey(false);
        return config;
    }
}
