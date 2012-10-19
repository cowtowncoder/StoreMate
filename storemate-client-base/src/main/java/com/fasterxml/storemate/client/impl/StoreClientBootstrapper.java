package com.fasterxml.storemate.client.impl;

import java.io.IOException;
import java.util.*;

import com.fasterxml.storemate.api.ClusterStatusResponse;
import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.api.NodeState;
import com.fasterxml.storemate.client.cluster.ClusterStatusAccessor;
import com.fasterxml.storemate.client.cluster.ClusterViewByClientImpl;
import com.fasterxml.storemate.client.cluster.Loggable;
import com.fasterxml.storemate.client.cluster.NetworkClient;
import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Helper class used for constructing and initializing a
 * {@link StoreClient} instance.
 */
public abstract class StoreClientBootstrapper<
    K extends EntryKey,
    CONFIG extends StoreClientConfig<K, CONFIG>,
    STORE extends StoreClient<K, CONFIG>,
    BOOTSTRAPPER extends StoreClientBootstrapper<K, CONFIG, STORE, BOOTSTRAPPER>
>
    extends Loggable
{
    /**
     * Let's keep initial timeouts relatively low, since we can usually
     * try to go through multiple server nodes to get response quickly.
     */
    public final static long BOOTSTRAP_TIMEOUT_MSECS = 2000L;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Helper objects
    ///////////////////////////////////////////////////////////////////////
     */

    protected final CONFIG _config;

    /**
     * Low-level library we use for making network requests.
     */
    protected final NetworkClient<K> _httpClient;
    
    /**
     * Set of server nodes used for bootstrapping; we need at least
     * one to be able to locate others.
     */
    protected final Set<IpAndPort> _nodes = new LinkedHashSet<IpAndPort>();

    protected ClusterStatusAccessor _accessor;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */
	
    public StoreClientBootstrapper(CONFIG config, NetworkClient<K> hc)
    {
        super(StoreClientBootstrapper.class);
        _config = config;
        _httpClient = hc;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Builder initialization
    ///////////////////////////////////////////////////////////////////////
     */
    
    @SuppressWarnings("unchecked")
    public BOOTSTRAPPER addNodes(IpAndPort... nodes)
    {
        for (IpAndPort node : nodes) {
            _nodes.add(node);
        }
        return (BOOTSTRAPPER) this;
    }

    @SuppressWarnings("unchecked")
    public BOOTSTRAPPER addNodes(String... nodes)
    {
        for (String node : nodes) {
            _nodes.add(new IpAndPort(node));
        }
        return (BOOTSTRAPPER) this;
    }

    @SuppressWarnings("unchecked")
    public BOOTSTRAPPER addNode(IpAndPort node) {
        _nodes.add(node);
        return (BOOTSTRAPPER) this;
    }

    public BOOTSTRAPPER addNode(String node) {
        return addNode(new IpAndPort(node));
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Client bootstrapping:
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Method to call to construct {@link StoreClient}, and ensure that it
     * can serve some requests: basically it will be able to contact at
     * least a single server node and thus handle at least part of the
     * key space.
     *<p>
     * If initialization can not be completed within 
     * 
     * @param maxWaitSecs Maximum time wait, in seconds, if more than 0; if
     *    0 or negative, will try to initialize indefinitely
     *    
     * @throws IllegalStateException If configuration is incomplete; for example
     * 	  if no server nodes have been configured
     */
    public STORE buildAndInitMinimally(int maxWaitSecs)
            throws IOException
    {
        return _buildAndInit(maxWaitSecs, false);
    }
    
    /**
     * Method to call to construct {@link StoreClient}, and ensure that it
     * can serve some requests: basically it will be able to contact at
     * least a single server node and thus handle at least part of the
     * key space.
     *    
     * @throws IllegalStateException If configuration is incomplete; for example
     * 	  if no server nodes have been configured
     */
    public STORE buildAndInitCompletely(int maxWaitSecs)
            throws IOException
    {
        return _buildAndInit(maxWaitSecs, true);
    }

    /**
     * Method called to verify that builder information is minimally valid,
     * to give early error information if possible.
     */
    protected void _verifySetup()
    {
        // First: check that we have at least one configured node:
        final int nodeCount = _nodes.size();
        if (nodeCount == 0) {
            throw new IllegalStateException("No server nodes defined for client, can not build");
        }
        // Second: try resolving IPs...
        Iterator<IpAndPort> it = _nodes.iterator();
        while (it.hasNext()) {
            IpAndPort ip = it.next();
            try {
                ip.getIP();
            } catch (Exception e) {
                logError("Failed to resolve end point '"+ip.toString()+"', skipping. Problem: "+e.getMessage());
                it.remove();
            }
        }
        // Still some left?
        if (_nodes.isEmpty()) {
            throw new IllegalStateException("Failed to resolve any of configured node definitions ("
                    +nodeCount+"): can not build");
        }
    }
    
    protected STORE _buildAndInit(int maxWaitSecs, boolean fullInit)
        throws IOException
    {
        _accessor = new ClusterStatusAccessor(_config.getJsonMapper());
        
        _verifySetup();
        
        final long startTime = System.currentTimeMillis();
        final long waitUntil = (maxWaitSecs <= 0) ? Long.MAX_VALUE : (startTime + 1000 * maxWaitSecs);
        // We'll keep track of other seed nodes:
        ArrayList<IpAndPort> ips = new ArrayList<IpAndPort>(_nodes);
        ClusterViewByClientImpl<K> clusterView = _getInitialState(_config, ips, waitUntil);

        // Nothing found before timeout? Bummer!
        if (clusterView == null) {
            // important: need to close client, otherwise exit from JVM won't work too well
            _accessor = null;
            throw new IllegalStateException("Failed to contact any of servers for Cluster Status: can not build client");
        }
        // This is the minimal state. Do we need more?
        boolean fullyAvailable = clusterView.isFullyAvailable();
        if (fullyAvailable) {
            if (isInfoEnabled()) {
                logInfo("Cluster information partially initialized; fully available: "+fullyAvailable);
            }
        } else if (fullInit) {
            while (true) {
                if (isInfoEnabled()) {
                    logInfo("Cluster information partially initialized, but only part of keyspace covered:"
                        +"need to continue initialization (have "+ips.size()+" nodes to check)");
                }
                // May need to loop for a while
                long roundStart = System.currentTimeMillis();
                
                if (_updateInitialState(ips, waitUntil, clusterView)) {
                    if (clusterView.isFullyAvailable()) {
                        break;
                    }
                    if (ips.isEmpty()) {
                        throw new IllegalStateException("Unable to fully initialize Cluster: all seed nodes handled; keyspace coverage incomplete");
                    }
                }
                // If we failed first time around, let's wait a bit...
                long now = System.currentTimeMillis();
                if (now > waitUntil) {
                    throw new IllegalStateException("Unable to fully initialize Cluster: keyspace coverage incomplete, timed out");
                }
                long timeTaken = now - roundStart;
                if (timeTaken < 3000L) { // if we had string of failures, wait a bit
                    try {
                        Thread.sleep(3000L - timeTaken);
                    } catch (InterruptedException e) {
                        throw new IOException(e.getMessage(), e);
                    }
                }
            }
            if (isInfoEnabled()) {
                logInfo("Cluster information completely initialized, the whole keyspace covered!");
            }
        }    
        return _buildClient(_config, _accessor, clusterView, _httpClient);
    }

    protected abstract STORE _buildClient(CONFIG config, ClusterStatusAccessor accessor,
            ClusterViewByClientImpl<K> clusterView, NetworkClient<K> client);
    
    /**
     * Method called to find information from the first available seed
     * server node.
     */
    protected ClusterViewByClientImpl<K> _getInitialState(CONFIG config,
            Collection<IpAndPort> nodes,
            long waitUntil) throws IOException
    {
        // First things first: must get one valid response first:
        long roundStart;
       
        while ((roundStart = System.currentTimeMillis()) < waitUntil) {
            Iterator<IpAndPort> it = nodes.iterator();
            while (it.hasNext()) {
                IpAndPort ip = it.next();
                final long requestTime = System.currentTimeMillis();
                long maxTimeout = waitUntil - requestTime;
                try {
                    ClusterStatusResponse resp = _accessor.getClusterStatus(ip,
                            Math.min(maxTimeout, BOOTSTRAP_TIMEOUT_MSECS));
                    if (resp == null) {
                        continue;
                    }
                    it.remove(); // remove from bootstrap list
                    NodeState local = resp.local;
                    ClusterViewByClientImpl<K> clusterView = new ClusterViewByClientImpl<K>(
                            _httpClient, local.totalRange().getKeyspace());
                    clusterView.updateDirectState(ip, local,
                            requestTime, System.currentTimeMillis(), resp.clusterLastUpdated);
                    for (NodeState stateSec : resp.remote) {
                        clusterView.updateIndirectState(ip, stateSec);
                    }
                    return clusterView;
                } catch (RuntimeException e) { // usually more severe ones, NPE etc
                    logError(e, "Internal error with cluster state call (IP "+ip+"): "
                            +"("+e.getClass().getName()+") "+e.getMessage());
                } catch (Exception e) {
                    logWarn("Initial cluster state call (IP "+ip+") failed: ("+e.getClass().getName()+") "
                            +e.getMessage());
                }
            }
            // If we failed first time around, let's wait a bit...
            long timeTaken = System.currentTimeMillis() - roundStart;
            if (timeTaken < 1000L) { // if we had string of failures, wait a bit
                try {
                    Thread.sleep(1000L - timeTaken);
                } catch (InterruptedException e) {
                    throw new IOException(e.getMessage(), e);
                }
            }
        }
        return null;
    }

    /**
     * Method that will try to fetch and resolve cluster state from one of
     * given nodes, to complete initialization wrt key space coverage
     * 
     * @return True if we managed to resolve one more node; false if not.
     */
    protected boolean _updateInitialState(Collection<IpAndPort> nodes,
            long waitUntil, ClusterViewByClientImpl<K> clusterView) throws IOException
    {
        Iterator<IpAndPort> it = nodes.iterator();
        while (it.hasNext()) {
            IpAndPort ip = it.next();
            final long requestTime = System.currentTimeMillis();
            long maxTimeout = waitUntil - requestTime;
            try {
                ClusterStatusResponse resp = _accessor.getClusterStatus(ip,
                        Math.min(maxTimeout, BOOTSTRAP_TIMEOUT_MSECS));
                if (resp == null) {
                    continue;
                }
                it.remove(); // remove from bootstrap list
                clusterView.updateDirectState(ip,  resp.local,
                        requestTime, System.currentTimeMillis(), resp.clusterLastUpdated);
                for (NodeState stateSec : resp.remote) {
                    clusterView.updateIndirectState(ip, stateSec);
                }
                return true;
            } catch (Exception e) {
                logWarn(e, "Secondary cluster state init call (IP "+ip+") failed: "+e.getMessage());
            }
        }
        return false;
    }
}
