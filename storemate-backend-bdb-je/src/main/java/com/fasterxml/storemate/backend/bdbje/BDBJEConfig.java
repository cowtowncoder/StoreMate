package com.fasterxml.storemate.backend.bdbje;

import java.io.File;

import com.fasterxml.storemate.store.backend.StoreBackendConfig;

import org.skife.config.DataAmount;

/**
 * Simple configuration class for BDB-JE - based backend.
 */
public class BDBJEConfig extends StoreBackendConfig
{
    /*
    /**********************************************************************
    /* Simple config properties, paths
    /**********************************************************************
     */

    /**
     * Name of root directory (using relative or absolute path) in which
     * metadata database will be created.
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
     * Size of BDB-JE cache, in bytes. Should be big enough to allow branches
     * to be kept in memory, but not necessarily the whole DB.
     *<p>
     * NOTE: most developers think "bigger is better", when it comes to cache
     * sizing. That is patently wrong idea -- too big a cache can kill
     * JVM via GC overhead. So a few megs goes a long way; the most important
     * cache is probably OS block cache for the file system.
     *<p>
     * Default value is 40 megs.
     */
    public DataAmount cacheSize = new DataAmount("40MB");

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    public BDBJEConfig() { }
    public BDBJEConfig(File dataRoot) {
        this.dataRoot = dataRoot;
    }
}
