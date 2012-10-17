package com.force.vagabond.client;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.storemate.api.KeyHash;
import com.fasterxml.storemate.api.KeyRange;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Value class used by {@link StoreClient} to keep track of state
 * of a single cluster node.
 * Instances are mutable to a degree, and properly synchronized to allow
 * thread-safe use.
 */
public class ServerNodeState
{
    /**
     * Address (ip number and port) used for communicating with the
     * node. Note that this is resolved end point in case server
     * node has been configured to use "localhost" (resolution can
     * be done since client has remote address for starting points)
     */
    private final IpAndPort _address;

    private KeyRange _activeRange;
    private KeyRange _passiveRange;
    private KeyRange _totalRange;

    /**
     * Reference to HTTP resource path used for CRUD operations
     */
    private final String _resourceEndpoint;
    
    protected final AtomicBoolean _disabled = new AtomicBoolean(false);

    /**
     * Time when last request was sent specifically for this server node
     * (i.e. not updated when we get indirect updates)
     */
    private final AtomicLong _lastRequestSent = new AtomicLong(0L);

    /**
     * Time when last request was sent specifically from this server node
     * (i.e. not updated when we get indirect updates)
     */
    private final AtomicLong _lastResponseReceived = new AtomicLong(0L);

    /**
     * Timestamp of last update for information regarding this node; regardless
     * of whether directly or indirectly.
     */
    private long _lastNodeUpdateFetched = 0L;

    /**
     * Timestamp of last version of cluster update from this server node
     * (i.e. not applicable for indirect updates)
     */
    private long _lastClusterUpdateFetched = 0L;

    /**
     * Timestamp of last version of cluster update that this server node
     * might have; received indirectly via one of GET, PUT or DELETE
     * operations.
     */
    private final AtomicLong _lastClusterUpdateAvailable = new AtomicLong(1L);

    /*
    ///////////////////////////////////////////////////////////////////////
    // Instance creation
    ///////////////////////////////////////////////////////////////////////
     */

    public ServerNodeState(IpAndPort address, KeyRange activeRange, KeyRange passiveRange)
    {
        _address = address;
        _activeRange = activeRange;
        _passiveRange = passiveRange;
        _totalRange = _activeRange.union(_passiveRange);
        _resourceEndpoint = address.getEndpoint(SMClientConfig.PATH_RESOURCE); 
    }

    protected static ServerNodeState forTesting(KeyRange range) {
        return forTesting(range, range);
    }

    protected static ServerNodeState forTesting(KeyRange rangeActive, KeyRange rangePassive) {
        return new ServerNodeState(new IpAndPort("localhost:"+rangeActive.getStart()),
                rangeActive, rangePassive);
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Mutations
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * @return True if ranges changed
     */
    public boolean updateRanges(KeyRange activeRange, KeyRange passiveRange)
    {
        if (_activeRange.equals(activeRange) && _passiveRange.equals(passiveRange)) {
            return false;
        }
        _activeRange = activeRange;
        _passiveRange = passiveRange;
        _totalRange = activeRange.union(passiveRange);
        return true;
    }

    /**
     * @return True if state changed
     */
    public boolean updateDisabled(boolean state) {
        boolean old = _disabled.getAndSet(state);
        return (old != state);
    }

    /**
     * Method called to update cluster info timestamps, based on header
     * received during regular CRUD operations.
     * 
     * @return True if state changed
     */
    public boolean updateLastClusterUpdateAvailable(long requestTime, long responseTime,
            long timestamp)
    {
        if (_lastResponseReceived.get() < responseTime) {
            _lastRequestSent.set(requestTime);
            _lastResponseReceived.set(responseTime);
            long old = _lastClusterUpdateAvailable.getAndSet(timestamp);
            return (old != timestamp);
        }
        return false;
    }
    
    public void setLastRequestSent(long timestamp) {
        _lastRequestSent.set(timestamp);
    }

    public void setLastResponseReceived(long timestamp) {
        _lastResponseReceived.set(timestamp);
    }

    public void setLastNodeUpdateFetched(long timestamp) {
        _lastNodeUpdateFetched = timestamp;
    }

    public void setLastClusterUpdateFetched(long timestamp) {
        _lastClusterUpdateFetched = timestamp;
    }

    public void setLastClusterUpdateAvailable(long timestamp) {
        _lastClusterUpdateAvailable.set(timestamp);
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Simple accessors
    ///////////////////////////////////////////////////////////////////////
     */

    public IpAndPort getAddress() { return _address; }

    public KeyRange getActiveRange() { return _activeRange; }
    public KeyRange getPassiveRange() { return _passiveRange; }
    public KeyRange getTotalRange() { return _totalRange; }

    public boolean isDisabled() { return _disabled.get(); }

    public long getLastRequestSent() { return _lastRequestSent.get(); }
    public long getLastResponseReceived() { return _lastResponseReceived.get(); }

    public long getLastNodeUpdateFetched() { return _lastNodeUpdateFetched; }
    public long getLastClusterUpdateFetched() { return _lastClusterUpdateFetched; }

    public long getLastClusterUpdateAvailable() { return _lastClusterUpdateAvailable.get(); }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Advanced accessors
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Method for calculating distance metrics to use for sorting server nodes
     * based on distance. In addition to basic clock-wise distance from start
     * point we also consider disabled nodes to be further away than any
     * enabled nodes; this is done by adding full length of key space to basic
     * clock-wise distance.
     */
    public int calculateSortingDistance(KeyHash keyHash)
    {
        /* Note: while we include things based on passive range, distance
         * should be based on active range; this to make passive range
         * more useful (can catch up with larger passive range; then enable
         * larger range once caught up)
         */
        KeyRange range = getActiveRange();
        int distance = range.clockwiseDistance(keyHash);
        if (isDisabled()) {
            distance += range.getKeyspace().getLength();
        }
        return distance;
    }

    public String resourceEndpoint() {
        return _resourceEndpoint;
    }
}
