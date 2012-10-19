package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.client.*;
import com.fasterxml.storemate.client.cluster.ServerNodeState;

public interface ContentPutter<K extends EntryKey>
{
    public CallFailure tryPut(CallConfig config, long endOfTime,
    		ServerNodeState server, K contentId, PutContentProvider content);
}
