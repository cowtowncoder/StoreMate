package com.fasterxml.storemate.client.cluster;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.api.EntryKeyConverter;
import com.fasterxml.storemate.api.RequestPathBuilder;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Factory abstraction used to separate details of physical network Client,
 * and logical functionality needed by higher-level client implementation.
 */
public abstract class NetworkClient<K extends EntryKey>
{
    public abstract RequestPathBuilder pathBuilder(IpAndPort server);

    /**
     * Method to call to shut down client implementation; called when
     * main client library is stopped.
     */
    public abstract void shutdown();

    /**
     * Accessor for factory method(s) for creating per-server accessor objects.
     */
    public abstract EntryAccessors<K> getEntryAccessors();

    public abstract EntryKeyConverter<K> getKeyConverter();
}
