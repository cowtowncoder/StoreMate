package com.fasterxml.storemate.client.cluster;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.client.call.ContentDeleter;
import com.fasterxml.storemate.client.call.ContentGetter;
import com.fasterxml.storemate.client.call.ContentHeader;
import com.fasterxml.storemate.client.call.ContentPutter;

/**
 * "Factory interface" that contains factory methods needed for constructing
 * accessors for CRUD operations on stored entries.
 */
public interface EntryAccessors<K extends EntryKey>
{
    public abstract ContentPutter<K> entryPutter(ClusterServerNode server);

    public abstract ContentGetter<K> entryGetter(ClusterServerNode server);

    public abstract ContentHeader<K> entryHeader(ClusterServerNode server);

    public abstract ContentDeleter<K> entryDeleter(ClusterServerNode server);

}
