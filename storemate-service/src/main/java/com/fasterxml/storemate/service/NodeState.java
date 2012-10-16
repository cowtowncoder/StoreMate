package com.fasterxml.storemate.service;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.key.KeyRange;

public abstract class NodeState
{
    /*
    ///////////////////////////////////////////////////////////////////////
    // Serialized state
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Public HTTP entry point (host, port) for the node.
     */
    public abstract IpAndPort getAddress();

    /**
     * Timestamp of last update by node itself to this
     * state information; propagated by other nodes, used
     * for determining most recent update. Always time from
     * node itself.
     */
    public abstract long getLastUpdated();
    
    public abstract KeyRange getRangeActive();

    public abstract KeyRange getRangePassive();

    public abstract KeyRange getRangeSync();
    
    public abstract boolean isDisabled();

    public abstract long getLastSyncAttempt();

    public abstract long getSyncedUpTo();
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Additional accessors
    ///////////////////////////////////////////////////////////////////////
     */

    
    public KeyRange totalRange() {
        KeyRange active = getRangeActive();
        KeyRange passive = getRangePassive();
        if (active == null) {
            return passive;
        }
        if (passive == null) {
            return active;
        }
        return active.union(passive);
    }
}
