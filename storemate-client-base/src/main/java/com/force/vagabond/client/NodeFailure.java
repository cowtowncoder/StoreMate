package com.force.vagabond.client;

import java.util.*;

import com.force.vagabond.client.ServerNodeState;

/**
 * Class that contains information about failure of an operation as it
 * relates to call(s) to a single server node.
 * Each instance contains at least one actual {@link CallFailure}.
 */
public class NodeFailure
{
    protected final ServerNodeState _server;

    protected LinkedList<CallFailure> _failures;
    
    public NodeFailure(ServerNodeState server, CallFailure firstFail)
    {
        _server = server;
        _failures = new LinkedList<CallFailure>();
        _failures.add(firstFail);
    }

    public void addFailure(CallFailure fail) {
        _failures.add(fail);
    }
    
    public ServerNodeState getServer() { return _server; }

    /**
     * Returns number of attempts that were made before giving up
     */
    public int getFailCount() { return _failures.size(); }

    public CallFailure getFirstCallFailure() { return _failures.getFirst(); }
    public CallFailure getLastCallFailure() { return _failures.getLast(); }

    public Iterable<CallFailure> getCallFailures() { return _failures; }

    @Override
    public String toString() {
        return "[Node Failure: "+_failures.size()+" failed; first = "+getFirstCallFailure()+"]";
    }

}

