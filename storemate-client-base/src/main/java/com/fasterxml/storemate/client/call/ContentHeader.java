package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.shared.EntryKey;

/**
 * Interface for a general purpose HEAD accessor, for a resource stored
 * in a single server; one accessor per server and end point.
 */
public interface ContentHeader<K extends EntryKey>
{
    /**
     * Method to call to try to make a single HEAD call to specified server
     * node.
     * 
     * @param config Configuration settings to use for call
     * @param endOfTime Time point at which the whole operation will time out
     * @param contentId Key of content to access
     */
    public HeadCallResult tryHead(CallConfig config, long endOfTime, K contentId);
}
