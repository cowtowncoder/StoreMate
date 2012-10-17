package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.api.ByteRange;
import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.client.*;

public interface ContentGetter<K extends EntryKey>
{
    public <T> GetCallResult<T> tryGet(CallConfig config, long endOfTime,
            ServerNodeState server, K contentId, GetContentProcessor<T> processor,
            ByteRange range);
}
