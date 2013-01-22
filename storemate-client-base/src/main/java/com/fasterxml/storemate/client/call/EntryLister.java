package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.ListType;

public interface EntryLister<K extends EntryKey>
{
    public <T> EntryListResult<T> tryList(CallConfig config, long endOfTime,
            K prefix, ListType type, int maxResults,
            ContentConverter<T> converter);
}
