package com.fasterxml.storemate.store;

import java.io.File;
import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Interface that defines callback used for operations
 * against file system
 *
 * @param <OUT> Type of return value
 */
public interface FileOperationCallback<OUT>
{
    /**
     * Callback method called with context.
     *
     * @param operationTime Start time of operation
     * @param key Key of entry being modified
     * @param value Optional value related to operation
     * 
     * @return Return value from operation, if any
     */
    public OUT perform(long operationTime, StorableKey key, Storable value,
            File externalFile)
        throws IOException, StoreException;
}
