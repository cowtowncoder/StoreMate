package com.fasterxml.storemate.store.lastaccess;

import org.skife.config.DataAmount;

/**
 * Configuration settings for the last-access store.
 */
public class LastAccessConfig
{
    /**
     * How much cache should be give the last-access store?
     */
    public DataAmount cacheSize = new DataAmount("20MB");
    
    /**
     * Should we use deferred writes for storing last-accessed information?
     * Note that use of deferred writes can cause data loss if system
     * crashes; during normal shutdown, <code>sync()</code> will be called
     * if deferred writes are enabled.
     *<p>
     * Default (of no value) is same as <code>false</code>, meaning that
     * deferred writes are not enabled.
     */
    public Boolean deferredWrites = null;

    /**
     * Default BDB-JE lock timeout (500 msecs) is bit short; let's up
     * the default to 5 seconds.
     */
    public int lockTimeoutMsecs = 5000;
    
    /*
    /**********************************************************************
    /* Convenience accessors
    /**********************************************************************
     */

    public boolean useDeferredWrites()
    {
        Boolean b = deferredWrites;
        return (b != null) && b.booleanValue();
    }
}
