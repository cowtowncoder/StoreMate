package com.fasterxml.storemate.store.backend;

/**
 * Value enumeration that indicates the method by which iteration
 * ended.
 */
public enum IterationResult
{
    /**
     * Returned when iteration due to call to {@link StorableIterationCallback#verifyKey}
     */
    TERMINATED_FOR_KEY,

    /**
     * Returned when iteration due to call to {@link StorableIterationCallback#processEntry}
     */
    TERMINATED_FOR_ENTRY,

    /**
     * Returned when iteration due to call to {@link StorableLastModIterationCallback#verifyTimestamp}
     */
    TERMINATED_FOR_TIMESTAMP,

    /**
     * Returned when iteration ended after going through the whole data set
     */
    FULLY_ITERATED
    ;
}
