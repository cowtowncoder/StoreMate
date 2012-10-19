package com.fasterxml.storemate.client.operation;

import java.util.LinkedList;

import com.fasterxml.storemate.client.cluster.ServerNodeState;

public class HeadOperationResult extends OperationResultImpl<HeadOperationResult>
{
    /**
     * Server that successfully delivered content, if any
     */
    protected ServerNodeState _server;

    /**
     * List of nodes that do not have entry for specified key.
     */
    protected LinkedList<ServerNodeState> _serversWithoutEntry = null;
    
    /**
     * Actual length fetched, if any
     */
    protected long _contentLength = -1L;
    
    public HeadOperationResult(OperationConfig config)
    {
        super(config);
    }
    
    public HeadOperationResult setContentLength(ServerNodeState server, long length)
    {
        if (_server != null) {
            throw new IllegalStateException("Already received successful response from "+_server+"; trying to override with "+server);
        }
        _server = server;
        _contentLength = length;
        return this;
    }

    /**
     * Method called to indicate that the requested entry was missing from
     * specified server. Some of information is included.
     * 
     * @param server Server that was missing requested entry
     */
    public HeadOperationResult addMissing(ServerNodeState server)
    {
        if (_serversWithoutEntry == null) {
            _serversWithoutEntry = new LinkedList<ServerNodeState>();
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

    public long getContentLength() { return _contentLength; }

    public int getMissingCount() {
        return (_serversWithoutEntry == null) ? 0 : _serversWithoutEntry.size();
    }
}
