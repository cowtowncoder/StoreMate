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
    public final static int DEFAULT_MIN_PAYLOAD_FOR_STREAMING = 64000;
    
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
