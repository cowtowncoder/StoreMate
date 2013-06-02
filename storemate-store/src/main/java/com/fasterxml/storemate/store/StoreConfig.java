package com.fasterxml.storemate.store;

import java.io.File;

import com.fasterxml.storemate.store.impl.StorableConverter;

/**
 * Simple configuration class for {@link StorableStore}
 */
public class StoreConfig
{
    // // Constants for tests
    
    public final static int DEFAULT_MAX_INLINED = 4000;
    public final static int DEFAULT_MIN_FOR_COMPRESS = 200;
    public final static int DEFAULT_MAX_FOR_GZIP = 16000;
    public final static int DEFAULT_MIN_PAYLOAD_FOR_STREAMING = 64000;

    /**
     * Default number of partitions in which local keyspace is sliced, for
     * purpose of locking per-entry access; balanced between granularity
     * of access (to minimize lock contention) and overhead of managing
     * locks. 64 seems like a reasonable initial guess.
     *<p>
     * Note that in addition to memory overhead, there is additional overhead
     * for finding latest in-flight timestamp, which is a linear operation.
     */
    public final static int DEFAULT_LOCK_PARTITIONS = 64;
    
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
    /* Simple config properties, numeric
    /**********************************************************************
     */

    /**
     * Per-entry access is protected by a hash-based partitioned mutex; more
     * partitions there are, lower is the expected lock contention, but
     * more memory overhead there is. Value must be power of 2; if not,
     * it will be rounded up to nearest such value.
     *<p>
     * Note that in addition to memory overhead, there is additional overhead
     * for finding latest in-flight timestamp, which is a linear operation.
     */
    public int lockPartitions = DEFAULT_LOCK_PARTITIONS;

    /*
    /**********************************************************************
    /* Simple config properties, paths
    /**********************************************************************
     */
        
    /**
     * Name of root directory (using relative or absolute path) under which
     * data files will be located (possibly with additional dir hierarchy).
     */
    public File dataRootForFiles;
    
    /*
    /**********************************************************************
    /* Simple config properties, size thresholds
    /**********************************************************************
     */
    
    /**
     * Maximum size of entries that are to be stored inline in the database,
     * instead of written out separately on file system.
     *<p>
     * Default value of about 4k is aligned to typical page size.
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

    /**
     * We will read up to this number of bytes in memory, before switching
     * to actual streaming handling. Note that streaming content will
     * never be inlined; and compression choice can not check whether
     * content is compressible (beyond basic compression prefix checks)
     */
    public int minPayloadForStreaming = DEFAULT_MIN_PAYLOAD_FOR_STREAMING;
    
    /*
    /**********************************************************************
    /* Overridable handlers
    /**********************************************************************
     */
    
    /**
     * {@link StorableConverter} implementation to use, if any
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
