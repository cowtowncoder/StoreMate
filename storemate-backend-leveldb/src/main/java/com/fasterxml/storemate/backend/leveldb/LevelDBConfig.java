package com.fasterxml.storemate.backend.leveldb;

import java.io.File;

import com.fasterxml.storemate.store.backend.StoreBackendConfig;

import org.skife.config.DataAmount;

/**
 * Simple configuration class for LevelDB - based backend.
 */
public class LevelDBConfig extends StoreBackendConfig
{
    /*
    /**********************************************************************
    /* Simple config properties, paths
    /**********************************************************************
     */

    /**
     * Name of root directory (using relative or absolute path) in which
     * data directions will be created (one for main DB, another for
     * last-modified index DB)
     *<p>
     * Should not be same as the directory in which data files are stored
     * (nor the main deployment directory)
     */
    public File dataRoot;
    
    /*
    /**********************************************************************
    /* Simple config properties, size thresholds
    /**********************************************************************
     */
    
    /**
     * Size of main data cache, in bytes. Should be big enough to allow branches
     * to be kept in memory, but not necessarily the whole DB.
     *<p>
     * Default value is 50 megs.
     */
    public DataAmount cacheSize = new DataAmount("50MB");
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    public LevelDBConfig() { }
    public LevelDBConfig(File dataRoot) {
        this(dataRoot, -1L);
    }

    public LevelDBConfig(File dataRoot, DataAmount cacheSize) {
        this.dataRoot = dataRoot;
        if (cacheSize != null) {
            this.cacheSize = cacheSize;
        }
    }
    
    public LevelDBConfig(File dataRoot, long cacheSizeInBytes) {
        this.dataRoot = dataRoot;
        if (cacheSizeInBytes > 0L) {
            cacheSize = new DataAmount(cacheSizeInBytes);
        }
    }

    /*
    /**********************************************************************
    /* Convenience stuff for overriding
    /**********************************************************************
     */

    public LevelDBConfig overrideCacheSize(long cacheSizeInBytes) {
        cacheSize = new DataAmount(cacheSizeInBytes);
        return this;
    }

    public LevelDBConfig overrideCacheSize(String cacheSizeDesc) {
        cacheSize = new DataAmount(cacheSizeDesc);
        return this;
    }
}
