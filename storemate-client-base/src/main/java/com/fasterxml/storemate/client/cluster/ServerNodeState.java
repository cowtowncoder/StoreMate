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
    /**
     * End point if the server node
     */
    public IpAndPort getAddress();

    public KeyRange getActiveRange();
    public KeyRange getPassiveRange();
    public KeyRange getTotalRange();

    /**
     * Whether server node is disabled: usually occurs during shutdowns
     * and startups, and is considered a transient state. Clients typically
     * try to avoid GET access from disabled nodes; and schedule updates
     * (if any) after all enabled instances.
     */
    public boolean isDisabled();

    /**
     * Timestamp when last node state access request was sent.
     */
    public long getLastRequestSent();

    /**
     * Timestamp when last node state access response was received (note:
     * does NOT include cases where error occured during request).
     */
    public long getLastResponseReceived();

    /**
     * Timestamp when last node state response update was processed.
     */
    public long getLastNodeUpdateFetched();

    /**
     * Timestamp of the last update that has been fetched from the server node.
     */
    public long getLastClusterUpdateFetched();

    /**
     * Timestamp of the latest update for the server node.
     */
    public long getLastClusterUpdateAvailable();

    /**
     * Accessor for finding URL for server endpoint used for
     * accessing (CRUD) of stored entries.
     */
    public <P extends RequestPathBuilder> P resourceEndpoint();
}
