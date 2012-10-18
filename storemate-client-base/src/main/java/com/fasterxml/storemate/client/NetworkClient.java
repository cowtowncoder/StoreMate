package com.fasterxml.storemate.client;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.api.RequestPathBuilder;
import com.fasterxml.storemate.client.call.ContentDeleter;
import com.fasterxml.storemate.client.call.ContentGetter;
import com.fasterxml.storemate.client.call.ContentHeader;
import com.fasterxml.storemate.client.call.ContentPutter;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Factory abstraction used to separate details of physical network Client,
 * and logical functionality needed by higher-level client implementation.
 */
public abstract class NetworkClient<K extends EntryKey>
{
	public abstract RequestPathBuilder pathBuilder(IpAndPort server);
	
    public abstract ContentHeader<K> contentHeader();
    public abstract ContentGetter<K> contentGetter();
    public abstract ContentPutter<K> contentPutter();
    public abstract ContentDeleter<K> contentDeleter();

    /**
     * Method to call to shut down client implementation; called when
     * main client library is stopped.
     */
    public abstract void shutdown();
}
