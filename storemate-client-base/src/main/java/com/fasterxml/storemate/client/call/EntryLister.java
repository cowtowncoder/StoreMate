package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.shared.EntryKey;

public interface EntryLister<K extends EntryKey>
{
    public <T> EntryListResult<T> tryList(CallConfig config, long endOfTime,
            K prefix, int maxResults, ContentConverter<T> converter);
}
