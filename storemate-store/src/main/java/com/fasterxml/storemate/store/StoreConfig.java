package com.fasterxml.storemate.store;

import com.fasterxml.storemate.store.file.FilenameConverter;

/**
 * Simple configuration class for {@link StorableStore}
 */
public class StoreConfig
{
    // // Constants for tests
    
    public final static int DEFAULT_MAX_INLINED = 4000;
    public final static int DEFAULT_MIN_FOR_COMPRESS = 200;
    public final static int DEFAULT_MAX_FOR_GZIP = 16000;
    
    /*
    /**********************************************************************
    /* Simple config properties, enable/disable
    /**********************************************************************
     */

    /**
     * Whether automatic compression of stored data is enabled or not.
     */
    public boolean compressionEnabled = true;

    /**
     * Whether checksum is required when storing pre-compressed entries,
     * for actual uncompressed content. If so, and caller does not provide
     * checksum, exception will be thrown. Otherwise checksum verification
     * can not be used.
     */
    public boolean requireChecksumForPreCompressed = true;

    /*
    /**********************************************************************
    /* Simple config properties, paths
    /**********************************************************************
     */
        
    /**
     * Name of root directory (using relative or absolute path) under which
     * actual data directories will be created.
     */
    public String dataRootPath;

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
     * sizing. That is patently wrong idea here -- too big cache can kill
     * JVM via GC overhead. So a few megs goes a long way; the most important
     * cache is probably OS block cache for the file system.
     *<p>
     * Default value is 40 megs.
     */
    public long cacheInBytes = 40 * 1024 * 1024;
    
    /**
     * Maximum size of entries that are to be stored inline in the database,
     * instead of written out separately on file system.
     *<p>
     * Defualt value of about 4k is aligned to typical page size.
     */
    public int maxInlinedStorageSize = DEFAULT_MAX_INLINED;

    /**
     * Minimum size an entry needs to have before we consider trying to
     * compress it. Low threshold used since smallest of content will
     * not compress (due to header overhead etc).
     */
    public int minUncompressedSizeForCompression = DEFAULT_MIN_FOR_COMPRESS;
    
    /**
     * Maximum uncompressed size of payload that will try to use GZIP
     * encoding; bigger payloads will use LZF due to reduced I/O costs
     * (and skippability of content).
     *<p>
     * Goal is to try to gzip inlined entries, use LZF for disk; and
     * assuming 4-to-1 compression we will use default size of 16k
     */
    public int maxUncompressedSizeForGZIP = DEFAULT_MAX_FOR_GZIP;
    
    /*
    /**********************************************************************
    /* Overridable handlers
    /**********************************************************************
     */
    
    /**
     * {@link FilenameConverter} implementation to use, if any
     */
    public Class<? extends StorableConverter> storableConverter = StorableConverter.class;
        
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */

    public StorableConverter createStorableConverter()
    {
        if (storableConverter == null || storableConverter == StorableConverter.class) {
            return new StorableConverter();
        }
        try {
            return (StorableConverter) storableConverter.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to instantiate StorableConverter of type "
                    +storableConverter+": "+e, e);
        }
    }

}
