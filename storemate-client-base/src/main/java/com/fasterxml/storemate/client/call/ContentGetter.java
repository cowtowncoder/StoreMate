package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.api.ByteRange;
import com.fasterxml.storemate.api.EntryKey;

/**
 * Interface for a general purpose GET accessor, for a resource stored
 * in a single server; one accessor per server and end point.
 */
public interface ContentGetter<K extends EntryKey>
{
    public <T> GetCallResult<T> tryGet(CallConfig config, long endOfTime,
            K contentId, GetContentProcessor<T> processor, ByteRange range);
}
