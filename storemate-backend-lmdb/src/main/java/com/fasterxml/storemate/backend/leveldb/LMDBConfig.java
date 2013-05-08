package com.fasterxml.storemate.backend.leveldb;

import java.io.File;

import com.fasterxml.storemate.store.backend.StoreBackendConfig;

import org.skife.config.DataAmount;

/**
 * Simple configuration class for LDBM - based backend.
 */
public class LMDBConfig extends StoreBackendConfig
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
    public DataAmount dataCacheSize = new DataAmount("50MB");

    /**
     * Size of cache for last-modified index. Should typically be smaller than
     * the main cache.
     *<p>
     * Default value is 20 megs.
     */
    public DataAmount indexCacheSize = new DataAmount("20MB");
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    public LMDBConfig() { }
    public LMDBConfig(File dataRoot) {
        this(dataRoot, -1L, -1L);
    }

    public LMDBConfig(File dataRoot, DataAmount dataCacheSize, DataAmount indexCacheSize) {
        this.dataRoot = dataRoot;
        if (dataCacheSize != null) {
            this.dataCacheSize = dataCacheSize;
        }
        if (indexCacheSize != null) {
            this.indexCacheSize = indexCacheSize;
        }
    }
    
    public LMDBConfig(File dataRoot, long dataCacheSizeInBytes, long indexCacheSizeInBytes) {
        this.dataRoot = dataRoot;
        if (dataCacheSizeInBytes > 0L) {
            dataCacheSize = new DataAmount(dataCacheSizeInBytes);
        }
        if (indexCacheSizeInBytes > 0L) {
            indexCacheSize = new DataAmount(indexCacheSizeInBytes);
        }
    }

    /*
    /**********************************************************************
    /* Convenience stuff for overriding
    /**********************************************************************
     */

    public LMDBConfig overrideDataCacheSize(long cacheSizeInBytes) {
        dataCacheSize = new DataAmount(cacheSizeInBytes);
        return this;
    }

    public LMDBConfig overrideIndexCacheSize(long cacheSizeInBytes) {
        indexCacheSize = new DataAmount(cacheSizeInBytes);
        return this;
    }

    public LMDBConfig overrideDataCacheSize(String cacheSizeDesc) {
        dataCacheSize = new DataAmount(cacheSizeDesc);
        return this;
    }

    public LMDBConfig overrideIndexCacheSize(String cacheSizeDesc) {
        indexCacheSize = new DataAmount(cacheSizeDesc);
        return this;
    }
}
