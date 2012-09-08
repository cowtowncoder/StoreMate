package com.fasterxml.storemate.store;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.*;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.bdb.LastModKeyCreator;
import com.fasterxml.storemate.store.bdb.PhysicalBDBStore;
import com.fasterxml.storemate.store.file.FileManager;

/**
 * Helper object used for configuring and instantiating
 * {@link StorableStore} instances.
 */
public class StoreBuilder
{
    protected FileManager _fileManager;
    protected TimeMaster _timeMaster;
    protected StoreConfig _config;

    protected StoreBuilder() { }
    public StoreBuilder(StoreConfig config,
            TimeMaster timeMaster, FileManager fileManager)
    {
        _config = config;
        _timeMaster = timeMaster;
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

        /*
torableConverter conv,
            File dbRoot, Database entryDB, SecondaryDatabase lastModIndex,
            StorableConverter converter,
            long bdbCacheSize)         */
        
        Environment env = new Environment(dbRoot, envConfig(canCreate, canWrite));
        Database entryDB = env.openDatabase(null, // no TX
                "entryMetadata", dbConfig(env));
        SecondaryDatabase index = env.openSecondaryDatabase(null, "lastModIndex", entryDB,
                indexConfig(env));
        PhysicalStore physicalStore = new PhysicalBDBStore(storableConv,
                dbRoot, entryDB, index, _config.cacheInBytes);

        try {
            StorableStore store = new StorableStore(_config, physicalStore,
                    _timeMaster, _fileManager);
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
