package com.fasterxml.storemate.store;

import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.file.FileReference;

/**
 * Helper class for containing information needed for storing
 * entries.
 */
public class StorableCreationMetadata
{
    /*
    /**********************************************************************
    /* Input data, provided by caller
    /**********************************************************************
     */

    /**
     * Size of content before compression, if known (-1 to indicate N/A);
     * may be provided by caller
     */
    public long uncompressedSize = -1L;

    /**
     * Murmur3/32 (seed 0) hash code on uncompressed content;
     * 0 means "not available"
     */
    public int contentHash;

    /**
     * Optional 
     * Murmur3/32 (seed 0) hash code on compressed content
     * 0 means "not available"
     */
    public int compressedContentHash;

    /**
     * Compression method used for content, if any. If left as null,
     * means "not known" and store can compress it as it sees fit;
     * if not null, server is NOT to do anything beyond possibly
     * verifying that content has valid signature for compression
     * method.
     */
    public Compression compression;

    /*
    /**********************************************************************
    /* Additional gathered state
    /**********************************************************************
     */

    public boolean deleted = false;
    
    /**
     * Timestamp used for the BDB entry to store
     */
    public long modtime;

    /**
     * For external entries, File in which actual data was written.
     */
    public FileReference dataFile;

    public long storageSize = -1L;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */
    
    public StorableCreationMetadata(Compression comp,
            int contentHash, int compressedContentHash)
    {
        compression = comp;
        this.contentHash = contentHash;
        this.compressedContentHash = compressedContentHash; 
    }
    
    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */

    public boolean usesCompression() {
        return (compression != null) && (compression != Compression.NONE);
    }

    public byte compressionAsByte() {
        if (compression == null) {
            return 0;
        }
        return (byte) compression.asIndex();
    }
    
    public byte statusAsByte() {
        if (deleted) {
            return 1;
        }
        return 0;
    }
}
