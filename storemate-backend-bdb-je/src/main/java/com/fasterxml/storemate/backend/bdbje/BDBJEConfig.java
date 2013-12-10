package com.fasterxml.storemate.backend.bdbje;

import java.io.File;

import com.fasterxml.storemate.store.backend.StoreBackendConfig;

import org.skife.config.DataAmount;

/**
 * Simple configuration class for BDB-JE - based backend.
 */
public class BDBJEConfig extends StoreBackendConfig
{
    /* Should we use transactions by default or not? Looks like
     * we should, as per BDB-JE documentation; but only because
     * of secondary indexes and their non-atomicity...
     */
    private final static boolean DEFAULT_USE_TRANSACTIONS = false;
    
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

    /**
     * Additional slash-separated path for storing "node state" under
     * Store metadata directory (which comes from main service config)
     */
    public String nodeStateDir = "nodes";
    
    /*
    /**********************************************************************
    /* Simple config properties, size thresholds
    /**********************************************************************
     */
    
    /**
     * Size of BDB-JE cache for entry metadata, in bytes.
     * Should be big enough to allow branches
     * to be kept in memory, but not necessarily the whole DB.
     *<p>
     * NOTE: most developers think "bigger is better", when it comes to cache
     * sizing. That is patently wrong idea -- too big a cache can kill
     * JVM via GC overhead. So a few megs goes a long way; the most important
     * cache is probably OS block cache for the file system.
     *<p>
     * Default value is 50 megs.
     */
    public DataAmount cacheSize = new DataAmount("50MB");
    
    /**
     * Configuration setting BDB-JE uses for
     * <code>EnvironmentConfig.LOCK_N_LOCK_TABLES</code> setting; BDB defaults
     * to 1, but typically a higher count should make sense. Let's start
     * with 17; probably not much benefit from super-high value since it ought
     * to be I/O bound.
     */
    public int lockTableCount = 17;

    /**
     * Length of timeout setting, in milliseconds, for BDB-JE to try to obtain
     * a lock on reading from store. This is per-environment setting so it
     * affects both lookups and by-mod-time iteration.
     *<p>
     * Default value of 7 seconds tries to balance requirements between detecting
     * and breaking deadlocks; failing "too slow" operations; and trying to prevent
     * unnecessary failures.
     * Current hypothesis is that this needs to be somewhat higher than what is
     * estimated as the slowest OldGen GC collection; and given baseline of 5
     * seconds max GC, adding bit of padding, 7 seems reasonable.
     */
    public int lockTimeoutMsecs = 7000;

    /*
    /**********************************************************************
    /* Simple config properties, on/off features
    /**********************************************************************
     */

    /**
     * Should we use deferred writes for entry metadata?
     * Note that use of deferred writes can cause data loss if system
     * crashes; during normal shutdown, <code>sync()</code> will be called
     * if deferred writes are enabled.
     *<p>
     * Default (of no value) is same as <code>false</code>, meaning that
     * deferred writes are <b>NOT</b> used.
     */
    public Boolean deferredWritesForEntries = null;

    /**
     * Should transactions be used for operations? Use is strongly recommended
     * only because we are using secondary indexes; and without transactions
     * it is possible (rare, unlikely, but possible) that secondary index
     * (last-modified index) can get out-of-sync, and result in the dreaded
     * "secondary index corrupt" exception when trying to expire entries.
     */
    public boolean useTransactions = DEFAULT_USE_TRANSACTIONS;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    public BDBJEConfig() { }
    public BDBJEConfig(File dataRoot) {
        this(dataRoot, -1L);
    }

    public BDBJEConfig(File dataRoot, DataAmount cacheSize) {
        this.dataRoot = dataRoot;
        if (cacheSize != null) {
            this.cacheSize = cacheSize;
        }
    }
    
    public BDBJEConfig(File dataRoot, long cacheSizeInBytes) {
        this.dataRoot = dataRoot;
        if (cacheSizeInBytes > 0L) {
            cacheSize = new DataAmount(cacheSizeInBytes);
        }
    }

    /*
    /**********************************************************************
    /* Convenience accessors
    /**********************************************************************
     */

    public boolean useDeferredWritesForEntries()
    {
        Boolean b = deferredWritesForEntries;
        return (b != null) && b.booleanValue();
    }
    
    /*
    /**********************************************************************
    /* Convenience stuff for overriding
    /**********************************************************************
     */

    public BDBJEConfig overrideCacheSize(long cacheSizeInBytes) {
        cacheSize = new DataAmount(cacheSizeInBytes);
        return this;
    }

    public BDBJEConfig overrideCacheSize(String cacheSizeDesc) {
        cacheSize = new DataAmount(cacheSizeDesc);
        return this;
    }

    public BDBJEConfig overrideLockTableCount(int count) {
        if (count < 1 || count > 1000) {
            throw new IllegalArgumentException("Illegal lockTableCount value ("+count+"); should be between [1, 1000]");
        }
        lockTableCount = count;
        return this;
    }

    /**
     * @since 0.9.6
     */
    public BDBJEConfig overrideLockTimeoutMsecs(int timeoutMsecs) {
        if (timeoutMsecs < 1) {
            throw new IllegalArgumentException("Illegal timeoutMsecs value ("+timeoutMsecs+"); should be a non-zero positive value");
        }
        lockTimeoutMsecs = timeoutMsecs;
        return this;
    }
}
