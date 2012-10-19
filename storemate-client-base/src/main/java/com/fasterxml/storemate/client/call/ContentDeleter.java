package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.client.CallFailure;

/**
 * Interface for a general purpose DELETE accessor, for a resource stored
 * in a single server; one accessor per server and end point.
 */
public interface ContentDeleter<K extends EntryKey>
{
    public CallFailure tryDelete(CallConfig config, long endOfTime, K key);
}
