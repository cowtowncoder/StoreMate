package com.fasterxml.storemate.store.bdb;

import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StoreException;

/**
 * Simple interface that defines behavior when trying to insert an
 * entry for key that already has entry.
 */
public interface OverwriteHandler
{
    /*
    /**********************************************************************
    /* API
    /**********************************************************************
     */

    public Response allowOverwrite(StorableKey key, StorableCreationMetadata metadata,
            Storable existingEntry)
        throws IOException, StoreException;

    /**
     * Enumeration of response values for {@link #allowOverwrite}.
     */
    public enum Response {
        /**
         * Response that indicates that entry should be overwritten
         */
        OVERWRITE,

        /**
         * Response that indicates that entry should be left as-is, but operation
         * considered successful.
         */
        LEAVE_BUT_SUCCEED,

        /**
         * Response that indicates that entry should be left as-is, and operation
         * considered a failure.
         */
        LEAVE_AND_FAIL
        ;
    };
}
