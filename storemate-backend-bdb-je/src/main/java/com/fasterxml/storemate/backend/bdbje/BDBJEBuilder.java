package com.fasterxml.storemate.backend.bdbje;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.skife.config.DataAmount;

import com.sleepycat.je.*;

import com.fasterxml.storemate.backend.bdbje.util.LastModKeyCreator;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreConfig;
import com.fasterxml.storemate.store.backend.StoreBackendBuilder;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;
import com.fasterxml.storemate.store.impl.StorableConverter;

/**
 * Helper object used for configuring and instantiating
 * {@link StorableStore} instances.
 */
public class BDBJEBuilder extends StoreBackendBuilder<BDBJEConfig>
{
    protected StoreConfig _storeConfig;
    protected BDBJEConfig _bdbConfig;

    public BDBJEBuilder() { this(null, null); }

    public BDBJEBuilder(StoreConfig storeConfig, BDBJEConfig bdbConfig)
    {
        super(BDBJEConfig.class);
        _storeConfig = storeConfig;
        _bdbConfig = bdbConfig;
    }

    @Override
    public BDBJEStoreBackend build() {
        return buildCreateAndInit();
    }

    /**
     * Method that will open an existing BDB database if one exists, or create
     * one if not, and create a store with that BDB. Underlying data storage
     * can do reads and writes.
     */
    public BDBJEStoreBackend buildCreateAndInit() {
        return _buildAndInit(true, true);
    }

    public BDBJEStoreBackend buildAndInitReadOnly() {
        return _buildAndInit(false, false);
    }

    public BDBJEStoreBackend buildAndInitReadWrite() {
        return _buildAndInit(false, true);
    }
    
    protected BDBJEStoreBackend _buildAndInit(boolean canCreate, boolean canWrite)
    {
        if (_storeConfig == null) throw new IllegalStateException("Missing StoreConfig");
        if (_bdbConfig == null) throw new IllegalStateException("Missing BDBJEConfig");

        File dbRoot = _bdbConfig.dataRoot;
        if (dbRoot == null) {
            throw new IllegalStateException("Missing BDBJEConfig.dataRoot");
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
        Environment env = new Environment(dbRoot, envConfig(canCreate, canWrite));
        Database entryDB = env.openDatabase(null, // no TX
                "entryMetadata", dbConfig(env));
        SecondaryDatabase index = env.openSecondaryDatabase(null, "lastModIndex", entryDB,
                indexConfig(env));
        BDBJEStoreBackend physicalStore = new BDBJEStoreBackend(storableConv,
                dbRoot, entryDB, index);

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
    
    @Override
    public BDBJEBuilder with(StoreConfig config) {
        _storeConfig = config;
        return this;
    }

    @Override
    public BDBJEBuilder with(StoreBackendConfig config) {
        if (!(config instanceof BDBJEConfig)) {
            String desc = (config == null) ? "NULL" : config.getClass().getName();
            throw new IllegalArgumentException("BDB-JE must be configured with a BDBJEConfig instance, not "
                    +desc);
        }
        _bdbConfig = (BDBJEConfig) config;
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
        config.setCacheSize(_bdbConfig.cacheSize.getNumberOfBytes());
        // Default of 500 msec way too low; usually set to higher value:
        config.setLockTimeout(_bdbConfig.lockTimeoutMsecs, TimeUnit.MILLISECONDS);
        // Default of 1 for lock count is not good; let's see what to use instead:
        config.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES, String.valueOf(_bdbConfig.lockTableCount));
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
        // important: timestamps are not unique, need to allow dups:
        config.setSortedDuplicates(true);
        // no, it is not immutable (entries will be updated with new timestamps)
        config.setImmutableSecondaryKey(false);
        return config;
    }
}
