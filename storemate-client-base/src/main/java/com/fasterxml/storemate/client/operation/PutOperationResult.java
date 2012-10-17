package com.fasterxml.storemate.client.operation;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.storemate.client.ServerNodeState;

public class PutOperationResult extends OperationResultImpl<PutOperationResult>
{
    /**
     * List of servers for which calls succeeded (possibly after initial failures and re-send),
     * in order of call completion.
     */
    protected final List<ServerNodeState> _succeeded;

    public PutOperationResult(OperationConfig config)
    {
        super(config);
        _succeeded = new ArrayList<ServerNodeState>(config.getOptimalOks());
    }

    public PutOperationResult addSucceeded(ServerNodeState server) {
        _succeeded.add(server);
        return this;
    }

    @Override
    public int getSuccessCount() { return _succeeded.size(); }
    public Iterable<ServerNodeState> getSuccessServers() { return _succeeded; }
}
