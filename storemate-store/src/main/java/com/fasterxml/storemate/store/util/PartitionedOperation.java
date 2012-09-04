package com.fasterxml.storemate.store.util;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Interface needed to implement to work with {@link PartitionedMonitor}.
 *
 * @param <IN> Type of argument being passed
 * @param <OUT> Type of return value from operation
 */
public interface PartitionedOperation<IN,OUT>
{
    public OUT perform(StorableKey key, IN arg) throws InterruptedException;
}
