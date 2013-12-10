package com.fasterxml.storemate.store.lastaccess;

import com.fasterxml.storemate.shared.util.ByteUtil;

/**
 * Default value container for simple last-accessed information, associated with
 * one or more entries. Entries are usually used for implementing dynamic time-to-live
 * based on when an entry (or one of entries of a group) is last-accessed.
 *<p>
 * Internal structure is:
 *<ol>
 * <li>#0: long lastAccessTime -- value used for TTL checks
 * <li>#8: long expirationTime -- timestamp when this entry is known to expire;
 *    meaning that it may be deleted if not collected earlier.
 * <li>#16: long type
 *</ol>
 * giving fixed length of 17 bytes for entries.
 */
public class EntryLastAccessed
{
    /**
     * Timestamp that indicates the last time entry was accessed (or,
     * for groups, last time any of entries was accessed).
     */
    public long lastAccessTime;

    /**
     * Timestamp that indicates creation time of content entry for which
     * last-accessed entry was last modified.
     * May be used for clean up purposes, to remove orphan entries.
     * Note that for group entries this just indicates one of many possible
     * creation times, so care has to be taken to consider if and how to use
     * it.
     */
    public long expirationTime;

    /**
     * Type of entry.
     */
    public byte type;

    public EntryLastAccessed(long accessTime, long expires, byte type) {
        lastAccessTime = accessTime;
        expirationTime = expires;
        this.type = type;
    }

    /**
     * Simple conversion method for serializing values.
     * Needs to be overridden by custom implementations.
     */
    public byte[] asBytes()
    {
        byte[] result = new byte[17];
        ByteUtil.putLongBE(result, 0, lastAccessTime);
        ByteUtil.putLongBE(result, 8, expirationTime);
        result[16] = type;
        return result;
    }

    /**
     * Overridable method called to determine if entry
     * is definitely expired; should return 'false' if expiration
     * status can not be reliably known. 
     */
    public boolean isExpired(long currentTime)
    {
        return (currentTime >= expirationTime);
    }
}
