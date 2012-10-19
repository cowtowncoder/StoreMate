package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.client.CallFailure;
import com.fasterxml.storemate.client.cluster.ServerNodeState;

public interface ContentDeleter<K extends EntryKey>
{
    public CallFailure tryDelete(CallConfig config, long endOfTime,
    		ServerNodeState server, K key);
}
