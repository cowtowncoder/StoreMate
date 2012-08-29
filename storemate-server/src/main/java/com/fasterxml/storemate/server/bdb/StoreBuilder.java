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
    protected Boolean _canWrite = Boolean.FALSE;

    public StoreBuilder() { }
    public StoreBuilder(StoreConfig config, FileManager fileManager)
    {
        _config = config;
        _fileManager = fileManager;
    }

    public StorableStore buildAndInit(boolean canCreate)
    {
        if (_config == null) throw new IllegalStateException("Missing StoreConfig");
        if (_fileManager == null) throw new IllegalStateException("Missing FileManager");

        File dbRoot = new File(_config.dataRootPath);
        if (!dbRoot.exists() || !dbRoot.isDirectory()) {
            if (!canCreate) {
                throw new IllegalArgumentException("Directory '"+dbRoot.getAbsolutePath()+"' does not exist, not allowed to (try to) create");
            }
            if (!dbRoot.mkdirs()) {
                throw new IllegalArgumentException("Directory '"+dbRoot.getAbsolutePath()+"' did not exist: failed to create it");
            }
        }
        Environment env = new Environment(dbRoot, envConfig(canCreate, _canWrite.booleanValue()));
        Database entryDB = env.openDatabase(null, // no TX
                "entryMetadata", dbConfig(env));
        SecondaryDatabase index = env.openSecondaryDatabase(null, "lastModIndex", entryDB,
                indexConfig(env));
        
        try {
            return new StorableStore(dbRoot, _fileManager, entryDB, index);
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

    public StoreBuilder readWriteStore() {
        _canWrite = Boolean.TRUE;
        return this;
    }

    public StoreBuilder readOnlyStore() {
        _canWrite = Boolean.FALSE;
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
        dbConfig.setAllowCreate(env.getConfig().getAllowCreate());
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
