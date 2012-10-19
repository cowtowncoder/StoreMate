package com.fasterxml.storemate.client.cluster;

import com.fasterxml.storemate.api.KeyRange;
import com.fasterxml.storemate.api.RequestPathBuilder;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Read-only part of logical server node state; used for exposing state
 * via result objects.
 */
public interface ServerNodeState
{
    public IpAndPort getAddress();

    public KeyRange getActiveRange();
    public KeyRange getPassiveRange();
    public KeyRange getTotalRange();

    public boolean isDisabled();

    public long getLastRequestSent();
    public long getLastResponseReceived();

    public long getLastNodeUpdateFetched();
    public long getLastClusterUpdateFetched();

    public long getLastClusterUpdateAvailable();

    /**
     * Accessor for finding URL for server endpoint used for
     * accessing (CRUD) of stored entries.
     */
    public <P extends RequestPathBuilder> P resourceEndpoint();
}
