package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.client.cluster.ServerNodeState;

public interface ContentHeader<K extends EntryKey>
{
    /**
     * Method to call to try to make a single HEAD call to specified server
     * node.
     * 
     * @param config Configuration settings to use for call
     * @param endOfTime Time point at which the whole operation will time out
     * @param server Information about server node to call
     * @param contentId Key of content to access
     */
    public HeadCallResult tryHead(CallConfig config, long endOfTime,
    		ServerNodeState server, K contentId);
}
