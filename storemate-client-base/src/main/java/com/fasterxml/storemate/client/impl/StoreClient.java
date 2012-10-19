package com.fasterxml.storemate.client.impl;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.storemate.api.ByteRange;
import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.api.EntryKeyConverter;
import com.fasterxml.storemate.client.*;
import com.fasterxml.storemate.client.call.*;
import com.fasterxml.storemate.client.cluster.ClusterStatusAccessor;
import com.fasterxml.storemate.client.cluster.ClusterViewByClient;
import com.fasterxml.storemate.client.cluster.Loggable;
import com.fasterxml.storemate.client.cluster.NetworkClient;
import com.fasterxml.storemate.client.cluster.NodesForKey;
import com.fasterxml.storemate.client.cluster.ClusterServerNode;
import com.fasterxml.storemate.client.operation.DeleteOperationResult;
import com.fasterxml.storemate.client.operation.GetOperationResult;
import com.fasterxml.storemate.client.operation.HeadOperationResult;
import com.fasterxml.storemate.client.operation.NodeFailure;
import com.fasterxml.storemate.client.operation.PutOperationResult;
import com.fasterxml.storemate.shared.util.ByteAggregator;

/**
 * Client used for accessing temporary store service.
 */
public abstract class StoreClient<K extends EntryKey,
    CONFIG extends StoreClientConfig<K, CONFIG>
>
    extends Loggable
{
    /**
     * This is just a simple "just-in-case" threshold to prevent message
     * flooding with retries; if things won't work with 5 retries (and initial
     * try, meaning 6 calls), we are probably hosed enough to give up
     * individual operations.
     */
    private final int MAX_RETRIES_FOR_PUT = 5;

    /**
     * This is just a simple "just-in-case" threshold to prevent message
     * flooding with retries.
     * Assuming client can still retry operation, use slightly lower
     * value than for PUTs
     */
    private final int MAX_RETRIES_FOR_GET = 3;

    /**
     * This is just a simple "just-in-case" threshold to prevent message
     * flooding with retries. Since DELETEs are bit more disposable,
     * let's use lower limit as well.
     */
    private final int MAX_RETRIES_FOR_DELETE = 3;

    /**
     * Limit calls for cluster status to once every two seconds
     */
    private final static long MIN_DELAY_BETWEEN_STATUS_CALLS_MSECS = 2000L;

    /**
     * Add modest amount of delay between rounds of calls when we have failures,
     * just to reduce congestion during overloads
     */ 
    private final static long DELAY_BETWEEN_RETRY_ROUNDS_MSECS = 250L;

    /**
     * Let's use Chunked Transfer-Encoding for larger payloads; cut-off
     * point is arbitrary, choose nice round number of 64k.
     */
    protected final static long MIN_LENGTH_FOR_CHUNKED = 64 * 1024;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Configuration
    ///////////////////////////////////////////////////////////////////////
     */

    protected final CONFIG _config;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Helper objects, HTTP
    ///////////////////////////////////////////////////////////////////////
     */

    protected final NetworkClient<K> _httpClient;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Helper objects, other
    ///////////////////////////////////////////////////////////////////////
     */

    protected final ClusterStatusAccessor _statusAccessor;

    protected EntryKeyConverter<K> _keyConverter;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Life-cycle
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Processing thread used for maintaining cluster state information.
     */
    protected Thread _thread;

    protected final AtomicBoolean _stopRequested = new AtomicBoolean(false);

    /*
    ///////////////////////////////////////////////////////////////////////
    // State
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected final ClusterViewByClient _clusterView;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Life-cycle
    ///////////////////////////////////////////////////////////////////////
     */

    protected StoreClient(CONFIG config,
            ClusterStatusAccessor statusAccessor, ClusterViewByClient clusterView,
            NetworkClient<K> httpClientImpl)
    {
        super(StoreClient.class);
        _config = config;
        _keyConverter = config.getKeyConverter();
        _httpClient = httpClientImpl;
        
        _statusAccessor = statusAccessor;
        _clusterView = clusterView;
    }

    /**
     * Method called by {@link StoreClientBootstrapper} once bootstrapping
     * is complete to some degree.
     */
    protected synchronized void start()
    {
        if (_thread != null) {
            throw new IllegalStateException("Trying to call start() more than once");
        }
        _stopRequested.set(false);
        Thread t = new Thread(new Runnable() {
            @Override public void run() {
                updateLoop();
            }
        });
        _thread = t;
        t.start();
    }
    
    /**
     * Method that must be called to stop processing thread client has
     */
    public void stop()
    {
        _stopRequested.set(true);
        Thread t = _thread;
        if (t != null) {
            t.interrupt();
        }
        // Should we ask HTTP Client to shut down here, or within thread?
        _httpClient.shutdown();
//        _blockingHttpClient.getConnectionManager().shutdown();
    }

    public synchronized boolean isRunning() {
        return (_thread != null);
    }

    public boolean hasStopBeenRequested() {
        return _stopRequested.get();
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Simple accessors
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Accessor for getting full cluster information
     */
    public ClusterViewByClient getCluster() {
        return _clusterView;
    }

    public EntryKeyConverter<K> getKeyConverter() {
        return _keyConverter;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Main update loop used for keeping up to date with Cluster Status
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Method that keeps on calling {@link #updateOnce} until Client is
     * requested to stop its operation.
     */
    protected void updateLoop()
    {
        try {
            while (!_stopRequested.get()) {
                final long startTime = System.currentTimeMillis();
                // throttle amount of work...
                final long nextCall = startTime + MIN_DELAY_BETWEEN_STATUS_CALLS_MSECS;
                try {
                    updateOnce();
                } catch (Exception e) {
                    logWarn(e, "Problem during Client updateLoop: "+e.getMessage());
                }
                long delay = nextCall - System.currentTimeMillis();
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) { }
                }
            }
        } finally {
            // after done, let's clear thread reference...
            synchronized (this) {
                _thread = null;
            }
        }
    }

    /**
     * Method that tries to update state of cluster by making a single call
     * to "most deserving" server node.
     */
    protected void updateOnce() throws Exception
    {
        // !!! TODO:
        // (1) Figure out which server node to call (least recently called one with updates etc)
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Actual Client API: convenience wrappers
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Convenience method for PUTting specified static content;
     * may be used if content need not be streamed from other sources.
     */
    public PutOperationResult putContent(CONFIG config, K key, byte[] data)
        throws InterruptedException {
        return putContent(config, key, data, 0, data.length);
    }

    /**
     * Convenience method for PUTting specified static content;
     * may be used if content need not be streamed from other sources.
     */
    public PutOperationResult putContent(CONFIG config, K key,
            byte[] data, int dataOffset, int dataLength)
        throws InterruptedException
    {
        return putContent(config, key, PutContentProviders.forBytes(data, dataOffset, dataLength));
    }
    
    /**
     * Convenience method for PUTting contents of specified File.
     */
    public PutOperationResult putContent(CONFIG config, K key,
            File file)
        throws InterruptedException
    {
        long fileLength = file.length();
        return putContent(config, key,
                PutContentProviders.forFile(file, fileLength));
    }
    
    /**
     * Convenience method for GETting specific content and aggregating it as a
     * byte array.
     * Note that failure to perform GET operation will be signaled with
     * {@link IllegalStateException}, whereas missing content is indicated by
     * null return value.
     * 
     * @return Array of bytes returned, if content exists; null if no such content
     *    exists (never PUT, or has been DELETEd)
     */
    public byte[] getContentAsBytes(CONFIG config, K key)
            throws InterruptedException
    {
        GetContentProcessorForBytes processor = new GetContentProcessorForBytes();
        GetOperationResult<ByteAggregator> result = getContent(config, key, processor);
        if (result.failed()) { // failed to contact any server
            _handleGetFailure(key, result);
        }
        // otherwise, we either got content, or got 404 or deletion
        ByteAggregator aggr = result.getContents();
        return (aggr == null) ? null : aggr.toByteArray();
    }

    /**
     * Convenience method for GETting specific content and storing it in specified file.
     * Note that failure to perform GET operation will be signaled with
     * {@link IllegalStateException}, whereas missing content is indicated by
     * 'false' return value
     * 
     * @return Original result file, if content exists; null if content was not found but
     *   operation succeeded (throw exception if access operation itself fails)
     */
    public File getContentAsFile(CONFIG config, K key, File resultFile)
            throws InterruptedException
    {
        GetContentProcessorForFiles processor = new GetContentProcessorForFiles(resultFile);
        GetOperationResult<File> result = getContent(config, key, processor);
        if (result.failed()) { // failed to contact any server
            _handleGetFailure(key, result);
        }
        // otherwise, we either got content, or got 404 or deletion -- latter means we return null:
        return result.getContents();
    }

    /**
     * Convenience method for GETting part of specified resource
     * aggregated as a byte array.
     *<p>
     * Note that failure to perform GET operation will be signaled with
     * {@link IllegalStateException}, whereas missing content is indicated by
     * null return value.
     *<p>
     * Note that when accessing ranges, content will always be return uncompressed
     * (if server compressed it, or received pre-compressed content declared with
     * compression type).
     * 
     * @param Specified byte range to access, using offsets in uncompressed content
     * 
     * @return Array of bytes returned, if content exists; null if no such content
     *    exists (never PUT, or has been DELETEd)
     */
    public byte[] getPartialContentAsBytes(CONFIG config, K key,
    		ByteRange range)
        throws InterruptedException
    {
        GetContentProcessorForBytes processor = new GetContentProcessorForBytes();
        GetOperationResult<ByteAggregator> result = getContent(config, key, processor, range);
        if (result.failed()) { // failed to contact any server
            _handleGetFailure(key, result);
        }
        // otherwise, we either got content, or got 404 or deletion
        ByteAggregator aggr = result.getContents();
        return (aggr == null) ? null : aggr.toByteArray();
    }

    /**
     * Convenience method for GETting part of specified resource
     * stored as specified file (if existing, will be overwritten).
     *<p>
     * Note that failure to perform GET operation will be signaled with
     * {@link IllegalStateException}, whereas missing content is indicated by
     * null return value.
     *<p>
     * Note that when accessing ranges, content will always be return uncompressed
     * (if server compressed it, or received pre-compressed content declared with
     * compression type).
     * 
     * @param Specified byte range to access, using offsets in uncompressed content
     * 
     * @return Original result file, if content exists; null if content was not found but
     *   operation succeeded (throw exception if access operation itself fails)
     */
    public File getPartialContentAsFile(CONFIG config, K key, File resultFile,
    		ByteRange range)
        throws InterruptedException
    {
        GetContentProcessorForFiles processor = new GetContentProcessorForFiles(resultFile);
        GetOperationResult<File> result = getContent(config, key, processor, range);
        if (result.failed()) { // failed to contact any server
            _handleGetFailure(key, result);
        }
        // otherwise, we either got content, or got 404 or deletion -- latter means we return null:
        return result.getContents();
    }
    
    /**
     * Convenience method for making HEAD request to figure out length of
     * the resource, if one exists (and -1 if not).
     * 
     * @return Length of entry in bytes, if entry exists: -1 if no such entry
     *    exists
     */
    public long getContentLength(CONFIG config, K key)
            throws InterruptedException
    {
        HeadOperationResult result = headContent(config, key);
        if (result.failed()) { // failed to contact any server
            NodeFailure nodeFail = result.getFirstFail();
            if (nodeFail != null) {
                CallFailure callFail = nodeFail.getFirstCallFailure();
                if (callFail != null) {
                    Throwable t = callFail.getCause();
                    if (t != null) {
                        throw new IllegalStateException("Failed to HEAD resource '"+key+"': tried and failed to access "
                                +result.getFailCount()+" server nodes; first failure due to: "+t);
                    }
                }
            }
            throw new IllegalStateException("Failed to HEAD resource '"+key+"': tried and failed to access "
                    +result.getFailCount()+" server nodes; first problem: "+result.getFirstFail());
        }
        return result.getContentLength();
    }
    
    protected void _handleGetFailure(K key, GetOperationResult<?> result)
    {
        NodeFailure nodeFail = result.getFirstFail();
        if (nodeFail != null) {
            CallFailure callFail = nodeFail.getFirstCallFailure();
            if (callFail != null) {
                Throwable t = callFail.getCause();
                if (t != null) {
                    throw new IllegalStateException("Failed to GET resource '"+key+"': tried and failed to access "
                            +result.getFailCount()+" server nodes; first failure due to: "+t);
                }
            }
        }
        throw new IllegalStateException("Failed to GET resource '"+key+"': tried and failed to access "
                +result.getFailCount()+" server nodes; first problem: "+result.getFirstFail());
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Actual Client API, low-level operations: PUT
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Method called to PUT specified content into appropriate server nodes.
     * 
     * @return Result object that indicates state of the operation as whole,
     *   including information on servers that were accessed during operation.
     *   Caller is expected to check details from this object to determine
     *   whether operation was successful or not.
     */
    public PutOperationResult putContent(CONFIG config, K key,
            PutContentProvider content)
        throws InterruptedException
    {
        final long startTime = System.currentTimeMillis();
        
        // First things first: find Server nodes to talk to:
        NodesForKey nodes = _clusterView.getNodesFor(key);
        PutOperationResult result = new PutOperationResult(config.getOperationConfig());

        // One sanity check: if not enough server nodes to talk to, can't succeed...
        int nodeCount = nodes.size();
        // should this actually result in an exception?
        if (nodeCount < config.getOperationConfig().getMinimalOksToSucceed()) {
            return result;
        }
        // Then figure out how long we have for the whole operation
        final long endOfTime = startTime + config.getOperationConfig().getGetOperationTimeoutMsecs();
        final long lastValidTime = endOfTime - config.getCallConfig().getMinimumTimeoutMsecs();

        /* Ok: first round; try PUT into every enabled store, up to optimal number
         * of successes we expect.
         */
        final boolean noRetries = !allowRetries();
        List<NodeFailure> retries = null;
        for (int i = 0; i < nodeCount; ++i) {
            ClusterServerNode server = nodes.node(i);
            if (server.isDisabled() && !noRetries) { // can skip disabled, iff retries allowed
                break;
            }
            CallFailure fail = server.entryPutter().tryPut(config.getCallConfig(), endOfTime, key, content);
            if (fail != null) { // only add to retry-list if something retry may help with
                if (fail.isRetriable()) {
                    retries = _add(retries, new NodeFailure(server, fail));
                } else {
                    result.addFailed(new NodeFailure(server, fail));
                }
                continue;
            }
            result.addSucceeded(server);
            // Very first round: go up to max if it's smooth sailing!
            if (result.succeededMaximally()) {
                return result.addFailed(retries);
            }
        }
        if (noRetries) { // if we can't retry, don't:
            return result.addFailed(retries);
        }

        // If we got this far, let's accept sub-optimal outcomes as well; or, if we timed out
        final long secondRoundStart = System.currentTimeMillis();
        if (result.succeededMinimally() || secondRoundStart >= lastValidTime) {
            return result.addFailed(retries);
        }
        // Do we need any delay in between?
        _doDelay(startTime, secondRoundStart, endOfTime);
        
        // Otherwise: go over retry list first, and if that's not enough, try disabled
        if (retries == null) {
            retries = new LinkedList<NodeFailure>();
        } else {
            Iterator<NodeFailure> it = retries.iterator();
            while (it.hasNext()) {
                NodeFailure retry = it.next();
                ClusterServerNode server = (ClusterServerNode) retry.getServer();
                CallFailure fail = server.entryPutter().tryPut(config.getCallConfig(), endOfTime, key, content);
                if (fail != null) {
                    retry.addFailure(fail);
                    if (!fail.isRetriable()) { // not worth retrying?
                        result.addFailed(retry);
                        it.remove();
                    }
                } else {
                    it.remove(); // remove now from retry list
                    result.addSucceeded(server);
                    if (result.succeededOptimally()) {
                        return result.addFailed(retries);
                    }
                }
            }
        }
        // if no success, add disabled nodes in the mix; but only if we don't have minimal success:
        for (int i = 0; i < nodeCount; ++i) {
            if (result.succeededMinimally() || System.currentTimeMillis() >= lastValidTime) {
                return result.addFailed(retries);
            }
            ClusterServerNode server = nodes.node(i);
            if (server.isDisabled()) {
                CallFailure fail = server.entryPutter().tryPut(config.getCallConfig(), endOfTime, key, content);
                if (fail != null) {
                    if (fail.isRetriable()) {
                        retries.add(new NodeFailure(server, fail));
                    } else {
                        result.addFailed(new NodeFailure(server, fail));
                    }
                } else {
                    result.addSucceeded(server);
                }
            }
        }

        // But from now on, keep on retrying, up to... N times (start with 1, as we did first retry)
        long prevStartTime = secondRoundStart;
        for (int i = 1; (i <= MAX_RETRIES_FOR_PUT) && !retries.isEmpty(); ++i) {
            final long currStartTime = System.currentTimeMillis();
            _doDelay(prevStartTime, currStartTime, endOfTime);
            // and off we go again...
            Iterator<NodeFailure> it = retries.iterator();
            while (it.hasNext()) {
                if (result.succeededMinimally() || System.currentTimeMillis() >= lastValidTime) {
                    return result.addFailed(retries);
                }
                NodeFailure retry = it.next();
                ClusterServerNode server = (ClusterServerNode) retry.getServer();
                CallFailure fail = server.entryPutter().tryPut(config.getCallConfig(), endOfTime, key, content);
                if (fail != null) {
                    retry.addFailure(fail);
                    if (!fail.isRetriable()) {
                        result.addFailed(retry);
                        it.remove();
                    }
                } else {
                    result.addSucceeded(server);
                }
            }
            prevStartTime = currStartTime;
        }
        // we are all done, failed:
        return result.addFailed(retries);
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Actual Client API, low-level operations: GET.
    // NOTE: division between streaming (incremental), full-read is ugly,
    // but somewhat necessary for efficient operation
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Method called to GET specified content from an appropriate server node,
     * and to pass it to specified processor for actual handling such as writing
     * to a file.
     * 
     * @return Result object that indicates state of the operation as whole,
     *   including information on servers that were accessed during operation.
     *   Caller is expected to check details from this object to determine
     *   whether operation was successful or not.
     */
    public <T> GetOperationResult<T> getContent(CONFIG config, K key,
            GetContentProcessor<T> processor)
        throws InterruptedException
    {
    	return getContent(config, key, processor, null);
    }

    public <T> GetOperationResult<T> getContent(CONFIG config, K key,
            GetContentProcessor<T> processor, ByteRange range)
        throws InterruptedException
    {
        final long startTime = System.currentTimeMillis();

        // First things first: find Server nodes to talk to:
        NodesForKey nodes = _clusterView.getNodesFor(key);
        // then result
        GetOperationResult<T> result = new GetOperationResult<T>(config.getOperationConfig());
        
        // One sanity check: if not enough server nodes to talk to, can't succeed...
        int nodeCount = nodes.size();
        if (nodeCount < 1) {
            return result; // or Exception?
        }
        
        // Then figure out how long we have for the whole operation
        final long endOfTime = startTime + config.getOperationConfig().getGetOperationTimeoutMsecs();
        final long lastValidTime = endOfTime - config.getCallConfig().getMinimumTimeoutMsecs();

        // Ok: first round; try GET from every enabled store
        final boolean noRetries = !allowRetries();
        List<NodeFailure> retries = null;
        for (int i = 0; i < nodeCount; ++i) {
            ClusterServerNode server = nodes.node(i);
            if (!server.isDisabled() || noRetries) {
                GetCallResult<T> gotten = server.entryGetter().tryGet(config.getCallConfig(), endOfTime, key,
                		processor, range);
                if (gotten.failed()) {
                    CallFailure fail = gotten.getFailure();
                    if (fail.isRetriable()) {
                        retries = _add(retries, new NodeFailure(server, fail));
                    } else {
                        result.addFailed(new NodeFailure(server, fail));
                    }
                    continue;
                }
                // did we get the thing?
                T entry = gotten.getResult();
                if (entry != null) {
                    return result.addFailed(retries).setContents(server, entry);
                }
                // it not, it's 404, missing entry. Neither fail nor really success...
                result = result.addMissing(server);
            }
        }
        if (noRetries) { // if we can't retry, don't:
            return result.addFailed(retries);
        }
        
        final long secondRoundStart = System.currentTimeMillis();
        // Do we need any delay in between?
        _doDelay(startTime, secondRoundStart, endOfTime);
        
        // Otherwise: go over retry list first, and if that's not enough, try disabled
        if (retries == null) {
            retries = new LinkedList<NodeFailure>();
        } else {
            Iterator<NodeFailure> it = retries.iterator();
            while (it.hasNext()) {
                NodeFailure retry = it.next();
                ClusterServerNode server = (ClusterServerNode) retry.getServer();
                GetCallResult<T> gotten = server.entryGetter().tryGet(config.getCallConfig(), endOfTime, key,
                		processor, range);
                if (gotten.succeeded()) {
                    T entry = gotten.getResult(); // got it?
                    if (entry != null) {
                        return result.addFailed(retries).setContents(server, entry);
                    }
                    // it not, it's 404, missing entry. Neither fail nor really success...
                    result = result.addMissing(server);
                    it.remove();
                } else {
                    CallFailure fail = gotten.getFailure();
                    retry.addFailure(fail);
                    if (!fail.isRetriable()) {
                        result.addFailed(retry);
                        it.remove();
                    }
                }
            }
        }
        // if no success, add disabled nodes in the mix
        for (int i = 0; i < nodeCount; ++i) {
            ClusterServerNode server = nodes.node(i);
            if (server.isDisabled()) {
                if (System.currentTimeMillis() >= lastValidTime) {
                    return result.addFailed(retries);
                }
                GetCallResult<T> gotten = server.entryGetter().tryGet(config.getCallConfig(), endOfTime, key,
                		processor, range);
                if (gotten.succeeded()) {
                    T entry = gotten.getResult(); // got it?
                    if (entry != null) {
                        return result.addFailed(retries).setContents(server, entry);
                    }
                    // it not, it's 404, missing entry. Neither fail nor really success...
                    result = result.addMissing(server);
                } else {
                    CallFailure fail = gotten.getFailure();
                    if (fail.isRetriable()) {
                        retries.add(new NodeFailure(server, fail));
                    } else {
                        result.addFailed(new NodeFailure(server, fail));
                    }
                }
            }
        }

        long prevStartTime = secondRoundStart;
        for (int i = 1; (i <= MAX_RETRIES_FOR_GET) && !retries.isEmpty(); ++i) {
            final long currStartTime = System.currentTimeMillis();
            _doDelay(prevStartTime, currStartTime, endOfTime);
            Iterator<NodeFailure> it = retries.iterator();
            while (it.hasNext()) {
                if (System.currentTimeMillis() >= lastValidTime) {
                    return result.addFailed(retries);
                }
                NodeFailure retry = it.next();
                ClusterServerNode server = (ClusterServerNode) retry.getServer();
                GetCallResult<T> gotten = server.entryGetter().tryGet(config.getCallConfig(), endOfTime, key,
                		processor, range);
                if (gotten.succeeded()) {
                    T entry = gotten.getResult(); // got it?
                    if (entry != null) {
                        return result.addFailed(retries).setContents(server, entry);
                    }
                    // it not, it's 404, missing entry. Neither fail nor really success...
                    result = result.addMissing(server);
                    it.remove();
                } else {
                    CallFailure fail = gotten.getFailure();
                    retry.addFailure(fail);
                    if (!fail.isRetriable()) {
                        result.addFailed(retry);
                        it.remove();
                    }
                }
            }
        }
        // we are all done and this'll be a failure...
        return result.addFailed(retries);
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Actual Client API, low-level operations: HEAD
    ///////////////////////////////////////////////////////////////////////
     */

    public HeadOperationResult headContent(CONFIG config, K key)
        throws InterruptedException
    {
        final long startTime = System.currentTimeMillis();

        // First things first: find Server nodes to talk to:
        NodesForKey nodes = _clusterView.getNodesFor(key);
        // then result
        HeadOperationResult result = new HeadOperationResult(config.getOperationConfig());
        
        // One sanity check: if not enough server nodes to talk to, can't succeed...
        int nodeCount = nodes.size();
        if (nodeCount < 1) {
            return result; // or Exception?
        }
        
        // Then figure out how long we have for the whole operation; use same timeout as GET
        final long endOfTime = startTime + config.getOperationConfig().getGetOperationTimeoutMsecs();
        final long lastValidTime = endOfTime - config.getCallConfig().getMinimumTimeoutMsecs();

        // Ok: first round; try HEAD from every enabled store (or, if only one try, all)
        final boolean noRetries = !allowRetries();
        List<NodeFailure> retries = null;
        for (int i = 0; i < nodeCount; ++i) {
            ClusterServerNode server = nodes.node(i);
            if (!server.isDisabled() || noRetries) {
                HeadCallResult gotten = server.entryHeader().tryHead(config.getCallConfig(), endOfTime, key);
                if (gotten.failed()) {
                    CallFailure fail = gotten.getFailure();
                    if (fail.isRetriable()) {
                        retries = _add(retries, new NodeFailure(server, fail));
                    } else {
                        result.addFailed(new NodeFailure(server, fail));
                    }
                    continue;
                }
                if (gotten.hasContentLength()) {
                    return result.addFailed(retries).setContentLength(server, gotten.getContentLength());
                }
                // it not, it's 404, missing entry. Neither fail nor really success...
                result = result.addMissing(server);
            }
        }
        if (noRetries) { // if no retries, bail out quickly
            return result.addFailed(retries);
        }
        
        final long secondRoundStart = System.currentTimeMillis();
        // Do we need any delay in between?
        _doDelay(startTime, secondRoundStart, endOfTime);
        
        // Otherwise: go over retry list first, and if that's not enough, try disabled
        if (retries == null) {
            retries = new LinkedList<NodeFailure>();
        } else {
            Iterator<NodeFailure> it = retries.iterator();
            while (it.hasNext()) {
                NodeFailure retry = it.next();
                ClusterServerNode server = (ClusterServerNode) retry.getServer();
                HeadCallResult gotten = server.entryHeader().tryHead(config.getCallConfig(), endOfTime, key);
                if (gotten.succeeded()) {
                    if (gotten.hasContentLength()) {
                        return result.addFailed(retries).setContentLength(server, gotten.getContentLength());
                    }
                    // it not, it's 404, missing entry. Neither fail nor really success...
                    result = result.addMissing(server);
                    it.remove();
                } else {
                    CallFailure fail = gotten.getFailure();
                    retry.addFailure(fail);
                    if (!fail.isRetriable()) {
                        result.addFailed(retry);
                        it.remove();
                    }
                }
            }
        }
        // if no success, add disabled nodes in the mix
        for (int i = 0; i < nodeCount; ++i) {
            ClusterServerNode server = nodes.node(i);
            if (server.isDisabled()) {
                if (System.currentTimeMillis() >= lastValidTime) {
                    return result.addFailed(retries);
                }
                HeadCallResult gotten = server.entryHeader().tryHead(config.getCallConfig(), endOfTime, key);
                if (gotten.succeeded()) {
                    if (gotten.hasContentLength()) {
                        return result.addFailed(retries).setContentLength(server, gotten.getContentLength());
                    }
                    // it not, it's 404, missing entry. Neither fail nor really success...
                    result = result.addMissing(server);
                } else {
                    CallFailure fail = gotten.getFailure();
                    if (fail.isRetriable()) {
                        retries.add(new NodeFailure(server, fail));
                    } else {
                        result.addFailed(new NodeFailure(server, fail));
                    }
                }
            }
        }

        long prevStartTime = secondRoundStart;
        for (int i = 1; (i <= MAX_RETRIES_FOR_GET) && !retries.isEmpty(); ++i) {
            final long currStartTime = System.currentTimeMillis();
            _doDelay(prevStartTime, currStartTime, endOfTime);
            Iterator<NodeFailure> it = retries.iterator();
            while (it.hasNext()) {
                if (System.currentTimeMillis() >= lastValidTime) {
                    return result.addFailed(retries);
                }
                NodeFailure retry = it.next();
                ClusterServerNode server = (ClusterServerNode) retry.getServer();
                HeadCallResult gotten = server.entryHeader().tryHead(config.getCallConfig(), endOfTime, key);
                if (gotten.succeeded()) {
                    if (gotten.hasContentLength()) {
                        return result.addFailed(retries).setContentLength(server, gotten.getContentLength());
                    }
                    // it not, it's 404, missing entry. Neither fail nor really success...
                    result = result.addMissing(server);
                    it.remove();
                } else {
                    CallFailure fail = gotten.getFailure();
                    retry.addFailure(fail);
                    if (!fail.isRetriable()) {
                        result.addFailed(retry);
                        it.remove();
                    }
                }
            }
        }
        // we are all done and this'll be a failure...
        return result.addFailed(retries);
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Actual Client API, low-level operations: DELETE
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Method called to DELETE specified content from appropriate server nodes.
     * 
     * @return Result object that indicates state of the operation as whole,
     *   including information on servers that were accessed during operation.
     *   Caller is expected to check details from this object to determine
     *   whether operation was successful or not.
     */
    public DeleteOperationResult deleteContent(CONFIG config, K key)
        throws InterruptedException
    {
        final long startTime = System.currentTimeMillis();

        // First things first: find Server nodes to talk to:
        NodesForKey nodes = _clusterView.getNodesFor(key);
        DeleteOperationResult result = new DeleteOperationResult(config.getOperationConfig());

        // One sanity check: if not enough server nodes to talk to, can't succeed...
        int nodeCount = nodes.size();
        if (nodeCount < config.getOperationConfig().getMinimalOksToSucceed()) {
            return result; // or Exception?
        }
        // Then figure out how long we have for the whole operation
        final long endOfTime = startTime + config.getOperationConfig().getGetOperationTimeoutMsecs();
        final long lastValidTime = endOfTime - config.getCallConfig().getMinimumTimeoutMsecs();

        /* Ok: first round; try DETE from every enabled store, up to optimal number
         * of successes we expect.
         */
        final boolean noRetries = !allowRetries();
        List<NodeFailure> retries = null;
        for (int i = 0; i < nodeCount; ++i) {
            ClusterServerNode server = nodes.node(i);
            if (server.isDisabled() && !noRetries) { // should be able to break, but let's double check
                break;
            }
            CallFailure fail = server.entryDeleter().tryDelete(config.getCallConfig(), endOfTime, key);
            if (fail != null) {
                if (fail.isRetriable()) {
                    retries = _add(retries, new NodeFailure(server, fail));
                } else {
                    result.addFailed(new NodeFailure(server, fail));
                }
                continue;
            }
            result.addSucceeded(server);
            // first round: go to the max, if possible
            if (result.succeededMaximally()) {
                return result.addFailed(retries);
            }
        }
        if (noRetries) { // if no retries, bail out quickly
            return result.addFailed(retries);
        }
        
        /* If we got this far, let's accept 'just optimal'; but keep on trying for
         * optimal since deletion via expiration is much more costly than explicit
         * DELETEs.
         */
        final long secondRoundStart = System.currentTimeMillis();
        if (result.succeededOptimally() || secondRoundStart >= lastValidTime) {
            return result.addFailed(retries);
        }
        // Do we need any delay in between?
        _doDelay(startTime, secondRoundStart, endOfTime);
        
        // Otherwise: go over retry list first, and if that's not enough, try disabled
        if (retries == null) {
            retries = new LinkedList<NodeFailure>();
        } else {
            Iterator<NodeFailure> it = retries.iterator();
            while (it.hasNext()) {
                NodeFailure retry = it.next();
                ClusterServerNode server = (ClusterServerNode) retry.getServer();
                CallFailure fail = server.entryDeleter().tryDelete(config.getCallConfig(), endOfTime, key);
                if (fail != null) {
                    retry.addFailure(fail);
                    if (!fail.isRetriable()) { // not worth retrying?
                        result.addFailed(retry);
                        it.remove();
                    }
                } else {
                    it.remove(); // remove now from retry list
                    result.addSucceeded(server);
                    if (result.succeededOptimally()) {
                        return result.addFailed(retries);
                    }
                }
            }
        }

        // if no success, add disabled nodes in the mix; but only if we don't have minimal success:
        for (int i = 0; i < nodeCount; ++i) {
            if (result.succeededMinimally() || System.currentTimeMillis() >= lastValidTime) {
                return result.addFailed(retries);
            }
            ClusterServerNode server = nodes.node(i);
            if (server.isDisabled()) {
                CallFailure fail = server.entryDeleter().tryDelete(config.getCallConfig(), endOfTime, key);
                if (fail != null) {
                    if (fail.isRetriable()) {
                        retries.add(new NodeFailure(server, fail));
                    } else {
                        result.addFailed(new NodeFailure(server, fail));
                    }
                } else {
                    result.addSucceeded(server);
                }
            }
        }

        long prevStartTime = secondRoundStart;
        for (int i = 1; (i <= MAX_RETRIES_FOR_DELETE) && !retries.isEmpty(); ++i) {
            final long currStartTime = System.currentTimeMillis();
            _doDelay(prevStartTime, currStartTime, endOfTime);
            // and off we go again...
            Iterator<NodeFailure> it = retries.iterator();
            while (it.hasNext()) {
                if (result.succeededMinimally() || System.currentTimeMillis() >= lastValidTime) {
                    return result.addFailed(retries);
                }
                NodeFailure retry = it.next();
                ClusterServerNode server = retry.getServer();
                CallFailure fail = server.entryDeleter().tryDelete(config.getCallConfig(), endOfTime, key);
                if (fail != null) {
                    retry.addFailure(fail);
                    if (!fail.isRetriable()) {
                        result.addFailed(retry);
                        it.remove();
                    }
                } else {
                    result.addSucceeded(server);
                }
            }
            prevStartTime = currStartTime;
        }
        // we are all done, failed:
        return result.addFailed(retries);
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Helper methods
    ///////////////////////////////////////////////////////////////////////
     */

    protected boolean allowRetries() {
        return _config.getOperationConfig().getAllowRetries();
    }
    
    protected <T> List<T> _add(List<T> list, T entry)
    {
        if (list == null) {
            list = new LinkedList<T>();
        }
        list.add(entry);
        return list;
    }
    
    protected void _doDelay(long startTime, long currTime, long endTime)
        throws InterruptedException
    {
        long timeSpent = currTime - startTime;
        // only add delay if we have had quick failures (signaling overload)
        if (timeSpent < 1000L) {
            long timeLeft = endTime - currTime;
            // also, only wait if we still have some time; and then modest amount (250 mecs)
            if (timeLeft >= (4 * DELAY_BETWEEN_RETRY_ROUNDS_MSECS)) {
                Thread.sleep(DELAY_BETWEEN_RETRY_ROUNDS_MSECS);
            }
        }
    }
}
