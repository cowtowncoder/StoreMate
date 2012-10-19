package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.client.*;

/**
 * Interface for a general purpose PUT accessor, for a resource stored
 * in a single server; one accessor per server and end point.
 */
public interface ContentPutter<K extends EntryKey>
{
    public CallFailure tryPut(CallConfig config, long endOfTime,
    		K contentId, PutContentProvider content);
}
