package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.client.CallFailure;

public interface ContentDeleter<K extends EntryKey>
{
    public CallFailure tryDelete(CallConfig config, long endOfTime, K key);
}
