package com.fasterxml.storemate.server.bdb;

/**
 * Simple configuration class for {@link StorableStore}
 */
public class StoreConfig
{
    /*
    /**********************************************************************
    /* Simple config properties
    /**********************************************************************
     */
        
    /**
     * Name of root directory (using relative or absolute path) under which
     * actual data directories will be created.
     */
    public String dataRootPath;

    /**
     * Size of BDB-JE cache, in bytes. Should be big enough to allow branches
     * to be kept in memory, but not necessarily the whole DB.
     *<p>
     * NOTE: most developers think "bigger is better", when it comes to cache
     * sizing. That is patently wrong idea here -- too big cache can kill
     * JVM via GC overhead. So a few megs goes a long way; the most important
     * cache is probably OS block cache for the file system.
     *<p>
     * Default value is 40 megs.
     */
    public long cacheInBytes = 40 * 1024 * 1024;
}
