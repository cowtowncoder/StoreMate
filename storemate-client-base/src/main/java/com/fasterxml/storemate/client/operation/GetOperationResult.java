package com.fasterxml.storemate.client.operation;

import java.util.LinkedList;

import com.fasterxml.storemate.client.call.GetContentProcessor;
import com.fasterxml.storemate.client.cluster.ClusterServerNode;

/**
 * {@link OperationResult} subtype used with GET operations, adds actual
 * result (of type <code>T</code> which is type of {@link GetContentProcessor}
 * passed to call).
 * 
 * @param <T> Result type of {@link GetContentProcessor}
 */
public class GetOperationResult<T> extends OperationResultImpl<GetOperationResult<T>>
{
    /**
     * Server that successfully delivered content, if any
     */
    protected ClusterServerNode _server;

    /**
     * List of nodes that do not have entry for specified key.
     */
    protected LinkedList<ClusterServerNode> _serversWithoutEntry = null;
    
    /**
     * Actual contents successfully fetched, if any
     */
    protected T _contents;
    
    public GetOperationResult(OperationConfig config)
    {
        super(config);
    }

    public GetOperationResult<T> setContents(ClusterServerNode server, T contents)
    {
        if (_server != null) {
            throw new IllegalStateException("Already received successful response from "+_server+"; trying to override with "+server);
        }
        _server = server;
        _contents = contents;
        return this;
    }

    /**
     * Method called to indicate that the requested entry was missing from
     * specified server. Some of information is included.
     * 
     * @param server Server that was missing requested entry
     */
    public GetOperationResult<T> addMissing(ClusterServerNode server)
    {
        if (_serversWithoutEntry == null) {
            _serversWithoutEntry = new LinkedList<ClusterServerNode>();
        }
        _serversWithoutEntry.add(server);
        return this;
    }
    
    @Override
    public int getSuccessCount() {
        if (_server != null || _serversWithoutEntry != null) {
            return 1;
        }
        return 0;
    }

    @Override
    public boolean succeededMinimally() {
        return getSuccessCount() > 0;
    }

    @Override
    public boolean succeededOptimally() {
        return getSuccessCount() > 0;
    }

    @Override
    public boolean succeededMaximally() {
        return getSuccessCount() > 0;
    }

    @Override
    protected void _addExtraInfo(StringBuilder sb) {
        sb.append(", missing: ").append(getMissingCount());
    }
    
    // // // Extended API

    public boolean failed() { return getSuccessCount() == 0; }
    public boolean succeeded() { return getSuccessCount() > 0; }

    public T getContents() { return _contents; }

    public int getMissingCount() {
        return (_serversWithoutEntry == null) ? 0 : _serversWithoutEntry.size();
    }
}

