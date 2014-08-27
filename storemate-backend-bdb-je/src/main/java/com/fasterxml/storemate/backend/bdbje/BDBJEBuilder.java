package com.fasterxml.storemate.backend.bdbje;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.*;

import com.fasterxml.storemate.shared.util.RawEntryConverter;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreConfig;
import com.fasterxml.storemate.store.backend.StoreBackendBuilder;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;
import com.fasterxml.storemate.store.impl.StorableConverter;
import com.fasterxml.storemate.store.state.NodeStateStore;

/**
 * Helper object used for configuring and instantiating
 * {@link StorableStore} instances.
 */
public class BDBJEBuilder extends StoreBackendBuilder<BDBJEConfig>
{
    /**
     * For Node stores we do not really need much any caching;
     * but throw dog a bone of, say, nice round 200k.
     */
    private final static long NODE_BDB_CACHE_SIZE = 200L * 1024L;
    
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

    @Override
    public <K,V> NodeStateStore<K,V> buildNodeStateStore(File metadataRoot,
            RawEntryConverter<K> keyConv,
            RawEntryConverter<V> valueConv)
    {
        verifyConfigs();
        if (metadataRoot == null) {
            throw new IllegalStateException("Missing 'metadataRoot'");
        }
        final String path = _bdbConfig.nodeStateDir;
        if (path == null || path.isEmpty()) {
            throw new IllegalStateException("Missing 'nodeStateDir'");
        }
        File nodeStateDir = _concatAndCreate(metadataRoot, path);
        Environment nodeEnv = new Environment(nodeStateDir, envConfigForNodeState(true, true));
        NodeStateStore<K,V> nodeStore;
        try {
            nodeStore = new BDBNodeStateStoreImpl<K,V>(null, keyConv, valueConv, nodeEnv);
        } catch (DatabaseException e) {
            String msg = "Failed to open Node store: "+e.getMessage();
            throw new IllegalStateException(msg, e);
        }
        return nodeStore;
    }

    @Override
    public <K,V> NodeStateStore<K,V> buildSecondaryNodeStateStore(File metadataRoot,
            String secondaryId,
            RawEntryConverter<K> keyConv, RawEntryConverter<V> valueConv)
    {
        if (secondaryId == null || secondaryId.isEmpty()) {
            throw new IllegalStateException("Missing argument 'secondaryId`");
        }
        verifyConfigs();
        if (metadataRoot == null) {
            throw new IllegalStateException("Missing 'metadataRoot'");
        }
        final String path = _bdbConfig.remoteStateDir;
        if (path == null || path.isEmpty()) {
            throw new IllegalStateException("Missing 'remoteStateDir'");
        }
        File nodeStateDir = _concatAndCreate(metadataRoot, path + "/" + secondaryId);
        Environment nodeEnv = new Environment(nodeStateDir, envConfigForNodeState(true, true));
        NodeStateStore<K,V> nodeStore;
        try {
            nodeStore = new BDBNodeStateStoreImpl<K,V>(null, keyConv, valueConv, nodeEnv);
        } catch (DatabaseException e) {
            String msg = "Failed to open Remote Node store (id '"+secondaryId+"'): "+e.getMessage();
            throw new IllegalStateException(msg, e);
        }
        return nodeStore;
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
        verifyConfigs();
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
        EnvironmentConfig envConfig = envConfigForStore(canCreate, canWrite);
        BDBJEStoreBackend physicalStore;
        try {
            physicalStore = new BDBJEStoreBackend(storableConv, dbRoot, _bdbConfig, envConfig);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to construct BDBJEStoreBackend: "+e.getMessage(), e);
        }
        try {
            physicalStore.start();
        } catch (DatabaseException e) {
            throw new IllegalStateException("Failed to start BDBJEStoreBackend: "+e.getMessage(), e);
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
    
    protected void verifyConfigs() {
        if (_storeConfig == null) throw new IllegalStateException("Missing StoreConfig");
        if (_bdbConfig == null) throw new IllegalStateException("Missing BDBJEConfig");
    }
    
    protected EnvironmentConfig envConfigForStore(boolean allowCreate, boolean writeAccess)
    {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(allowCreate);
        config.setReadOnly(!writeAccess);
        config.setTransactional(_bdbConfig.useTransactions);
        config.setSharedCache(false);
        config.setCacheSize(_bdbConfig.cacheSize.getNumberOfBytes());
        // Default of 500 msec way too low; usually set to higher value:
        config.setLockTimeout(_bdbConfig.lockTimeoutMsecs, TimeUnit.MILLISECONDS);
        // Default of 1 for lock count is not good; let's see what to use instead:
        config.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES, String.valueOf(_bdbConfig.lockTableCount));
        return config;
    }

    protected static EnvironmentConfig envConfigForNodeState(boolean allowCreate, boolean writeAccess)
    {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(allowCreate);
        config.setReadOnly(!writeAccess);
        config.setSharedCache(false);
        config.setCacheSize(NODE_BDB_CACHE_SIZE);
        config.setDurability(Durability.COMMIT_SYNC);
        // default of 500 msec too low; although for node settings should not really matter:
        config.setLockTimeout(5000L, TimeUnit.MILLISECONDS);
        return config;
    }
}
