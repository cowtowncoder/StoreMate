package com.fasterxml.storemate.store.impl;

public class StorableFlags
{
    /**
     * Status flag that indicates whether entry has been soft-deleted
     * or not.
     */
    public final static int F_STATUS_SOFT_DELETED = 0x01;

    /**
     * Status flag that indicates whether entry was directly uploaded
     * by client (false) or replicated via synchronization (true)
     */
    public final static int F_STATUS_REPLICATED = 0x02;

    // // // Helper constants for bit-masking
    
    public final static byte NOT_SOFT_DELETED = (byte) (~ F_STATUS_SOFT_DELETED);
    
    public final static byte NOT_REPLICATED = (byte) (~ F_STATUS_REPLICATED);

    private StorableFlags() { }
}
