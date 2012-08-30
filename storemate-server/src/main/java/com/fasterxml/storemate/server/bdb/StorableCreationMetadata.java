package com.fasterxml.storemate.server.bdb;

import com.fasterxml.storemate.shared.compress.Compression;

/**
 * Helper class for containing information needed for storing
 * entries.
 */
public class StorableCreationMetadata
{
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
}
