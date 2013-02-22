package com.fasterxml.storemate.store.util;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.*;

/**
 * Callback interface used with calls that may try to overwrite existing entries;
 * and if so may decide whether overwrite should or should not proceed.
 */
public interface OverwriteChecker
{
    /**
     * Method that may be called at first, to see if ovewrite for given key is
     * always allowed ({@link Boolean#TRUE}), never allowed ({@link Boolean#FALSE}),
     * or "depends on entries involved" (null).
     */
    public Boolean mayOverwrite(StorableKey key);
    
    /**
     * Method called to determine whether specified overwrite operation should succeeed
     * or not.
     * Note that checker will only have access to metadata, not data itself, since
     * reading data may incur significant overhead and/or buffering.
     * 
     * @return Whether specified existing entry may be overridden by specified new entry (or not).
     *     
     * @throws StoreException May be thrown to indicate that overwrite operation
     *   is not allowed, as an alternative to returning specific fail
     */
    public boolean mayOverwrite(StorableKey key, Storable oldEntry, Storable newEntry)
        throws StoreException;

    /*
    /**********************************************************************
    /* Standard implementations
    /**********************************************************************
     */

    /**
     * Simple implementation that simply allows all overwrites.
     */
    public static class AlwaysOkToOverwrite implements OverwriteChecker
    {
        public final static OverwriteChecker instance = new AlwaysOkToOverwrite();
        
        @Override
        public Boolean mayOverwrite(StorableKey key) { return Boolean.TRUE; }

        @Override
        public boolean mayOverwrite(StorableKey key, Storable oldEntry, Storable newEntry)
            throws StoreException {
            return true;
        }
    }

    /**
     * Simple implementation that simply denies all overwrites.
     */
    public static class NeverOkToOverwrite implements OverwriteChecker
    {
        public final static OverwriteChecker instance = new NeverOkToOverwrite();

        @Override
        public Boolean mayOverwrite(StorableKey key) { return Boolean.FALSE; }
        
        @Override
        public boolean mayOverwrite(StorableKey key, Storable oldEntry, Storable newEntry)
            throws StoreException {
            return false;
        }
    }
}
