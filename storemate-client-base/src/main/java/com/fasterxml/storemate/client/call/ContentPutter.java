package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.client.*;

public interface ContentPutter<K extends EntryKey>
{
    public CallFailure tryPut(CallConfig config, long endOfTime,
    		ServerNodeState server, K contentId, PutContentProvider content);
}
