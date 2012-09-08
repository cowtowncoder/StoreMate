package com.fasterxml.storemate.backend.bdbje;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.*;

import com.fasterxml.storemate.store.StorableConverter;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreConfig;

/**
 * Helper object used for configuring and instantiating
 * {@link StorableStore} instances.
 */
public class BDBJEBuilder
{
    protected StoreConfig _config;

    protected BDBJEBuilder() { }
    public BDBJEBuilder(StoreConfig config)
    {
        _config = config;
    }

    /**
     * Method that will open an existing BDB database if one exists, or create
     * one if not, and create a store with that BDB. Underlying data storage
     * can do reads and writes.
     */
    public PhysicalBDBStore buildCreateAndInit() {
        return _buildAndInit(true, true);
    }

    public PhysicalBDBStore buildAndInitReadOnly() {
        return _buildAndInit(false, false);
    }

    public PhysicalBDBStore buildAndInitReadWrite() {
        return _buildAndInit(false, true);
    }
    
    protected PhysicalBDBStore _buildAndInit(boolean canCreate, boolean canWrite)
    {
        if (_config == null) throw new IllegalStateException("Missing StoreConfig");
        
        File dbRoot = new File(_config.dataRootPath);
        if (!dbRoot.exists() || !dbRoot.isDirectory()) {
            if (!canCreate) {
                throw new IllegalArgumentException("Directory '"+dbRoot.getAbsolutePath()+"' does not exist, not allowed to (try to) create");
            }
            if (!dbRoot.mkdirs()) {
                throw new IllegalArgumentException("Directory '"+dbRoot.getAbsolutePath()+"' did not exist: failed to create it");
            }
        }

        /* !!! TODO: make PhysicalStore configurable as well...
         */

        StorableConverter storableConv = _config.createStorableConverter();
        Environment env = new Environment(dbRoot, envConfig(canCreate, canWrite));
        Database entryDB = env.openDatabase(null, // no TX
                "entryMetadata", dbConfig(env));
        SecondaryDatabase index = env.openSecondaryDatabase(null, "lastModIndex", entryDB,
                indexConfig(env));
        PhysicalBDBStore physicalStore = new PhysicalBDBStore(storableConv,
                dbRoot, entryDB, index, _config.cacheInBytes);

        try {
        	physicalStore.start();
        } catch (DatabaseException e) {
            throw new IllegalStateException("Failed to open StorableStore: "+e.getMessage(), e);
        }
        return physicalStore;
    }

    /*
    /**********************************************************************
    /* Fluent methods
    /**********************************************************************
     */
    
    public BDBJEBuilder with(StoreConfig config) {
        _config = config;
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
        /* Default of 500 msec way too low, let's see if 5 seconds works
         * better.
         */
        config.setLockTimeout(5000L, TimeUnit.MILLISECONDS);
        // Default of 1 for lock count is not good; must be prime, so let's try 7
        config.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES, "7");
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
