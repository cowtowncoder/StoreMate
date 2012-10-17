package com.fasterxml.storemate.api;

import com.fasterxml.storemate.api.KeyRange;
import com.fasterxml.storemate.shared.IpAndPort;

public class NodeState
{
    protected IpAndPort _address;
    protected long _lastUpdated;
    protected KeyRange _rangeActive;
    protected KeyRange _rangePassive;
    protected KeyRange _rangeSync;
    protected boolean _isDisabled;
    protected long _lastSyncAttempt;
    protected long _syncedUpTo;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Accessors
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Public HTTP entry point (host, port) for the node.
     */
    public IpAndPort getAddress() {
        return _address;
    }

    /**
     * Timestamp of last update by node itself to this
     * state information; propagated by other nodes, used
     * for determining most recent update. Always time from
     * node itself.
     */
    public long getLastUpdated() {
        return _lastUpdated;
    }
    
    public KeyRange getRangeActive() {
        return _rangeActive;
    }

    public KeyRange getRangePassive() {
        return _rangePassive;
    }

    public KeyRange getRangeSync() {
        return _rangeSync;
    }
    
    public boolean isDisabled() {
        return _isDisabled;
    }

    public long getLastSyncAttempt() {
        return _lastSyncAttempt;
    }

    public long getSyncedUpTo() {
        return _syncedUpTo;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Mutators (for deserialization only; ok to be protected)
    ///////////////////////////////////////////////////////////////////////
     */

    public void setAddress(IpAndPort v) {
        _address = v;
    }

    public void setLastUpdated(long v) {
        _lastUpdated = v;
    }
    
    public void setRangeActive(KeyRange v) {
        _rangeActive = v;
    }

    public void setRangePassive(KeyRange v) {
        _rangePassive = v;
    }

    public void setRangeSync(KeyRange v) {
        _rangeSync = v;
    }
    
    public void setDisabled(boolean v) {
        _isDisabled = v;
    }

    public void setLastSyncAttempt(long v) {
         _lastSyncAttempt = v;
    }

    public void setSyncedUpTo(long v) {
        _syncedUpTo = v;
    }
    
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
