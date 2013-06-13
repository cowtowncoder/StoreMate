package com.fasterxml.storemate.store;

/**
 * Enumeration used to indicate type of higher-level operation
 * (like request) that result in operation being called.
 * This is necessary for {@link StoreOperationThrottler} to properly
 * handle throttling aspects and balance needs of requests as well
 * as background batch processes.
 */
public enum StoreOperationSource
{
    /**
     * Operation is taken to fulfill an external request
     */
    REQUEST,

    /**
     * Operation is taken as part of synchronization processing
     * (either sync list or sync pull)
     */
    SYNC,

    /**
     * Operation taken as part of background cleanup process.
     */
    CLEANUP
    ;
}
