package com.fasterxml.storemate.store;

import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.file.FileReference;
import com.fasterxml.storemate.store.impl.StorableFlags;

/**
 * Helper class for containing information needed for storing
 * entries.
 */
public class StorableCreationMetadata
    implements Cloneable
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

    public boolean deleted;

    public boolean replicated;

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

    @Override
    public StorableCreationMetadata clone() {
        try {
            return (StorableCreationMetadata) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed clone(): "+e.getMessage(), e);
        }
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

    public byte statusAsByte()
    {
        int status = deleted ? StorableFlags.F_STATUS_SOFT_DELETED : 0;
        if (replicated) {
            status |= StorableFlags.F_STATUS_REPLICATED;
        }
        return (byte) status;
    }
}
