package com.fasterxml.storemate.client.cluster;

import com.fasterxml.storemate.api.EntryKey;

public abstract class ClusterViewByClient
{
    /**
     * Accessor for finding out number of server nodes cluster is known
     * to have.
     */
    public abstract int getServerCount();

    /**
     * Method that can be called to determine whether at least one node
     * exists to access all keys of cluster's key space. Currently
     * this accepts both active and passive ranges, as well as disabled
     * nodes; mostly because method is called during bootstrapping when
     * we are typically interested in possibility of everything being
     * accessible.
     */
    public abstract boolean isFullyAvailable();

    /**
     * Method for accessing exact coverage that we currently have for key space;
     * returns number of hash slots that are covered by at least one node.
     */
    public abstract int getCoverage();

    /**
     * Method for finding set of nodes to contact for accessing content with
     * specified key.
     */
    public abstract NodesForKey getNodesFor(EntryKey key);
}
