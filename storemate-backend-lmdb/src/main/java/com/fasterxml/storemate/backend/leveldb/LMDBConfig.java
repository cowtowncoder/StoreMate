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
     * NOTE: LMDB does not believe in lib-level caching, nothing
     * to configure
     */
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    public LMDBConfig() { }
    public LMDBConfig(File dataRoot) {
        this.dataRoot = dataRoot;
    }
}
