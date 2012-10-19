package com.fasterxml.storemate.client.operation;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.storemate.client.cluster.ClusterServerNode;

public class DeleteOperationResult extends OperationResultImpl<DeleteOperationResult>
{
    /**
     * List of servers for which calls succeeded (possibly after initial failures and re-send),
     * in order of call completion.
     */
    protected final List<ClusterServerNode> _succeeded;
    
    public DeleteOperationResult(OperationConfig config)
    {
        super(config);
        _succeeded = new ArrayList<ClusterServerNode>(config.getOptimalOks());
    }

    public DeleteOperationResult addSucceeded(ClusterServerNode server) {
        _succeeded.add(server);
        return this;
    }

    @Override
    public int getSuccessCount() { return _succeeded.size(); }

    public Iterable<ClusterServerNode> getSuccessServers() { return _succeeded; }
}
