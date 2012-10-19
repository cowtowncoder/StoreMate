package com.fasterxml.storemate.client;

import com.fasterxml.storemate.client.cluster.ServerNodeState;
import com.fasterxml.storemate.client.operation.OperationConfig;

/**
 * Class used for returning information about operation success (or lack thereof).
 */
public abstract class OperationResult<T extends OperationResult<T>>
{
    protected OperationResult() { }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Accessors
    ///////////////////////////////////////////////////////////////////////
     */
    
    public abstract OperationConfig getConfig();

    /**
     * Simple accessor for checking whether call succeeded to minimum degree
     * required, or not. This means that we had at least minimal required number
     * of succesful individual calls.
     */
    public abstract boolean succeededMinimally();

    /**
     * Simple accessor for checking whether call succeeded well
     * enough that we may consider it full success.
     * We may either choose to do more updates (if nodes are available);
     * up to {@link #succeededMaximally()} level; or just return
     * and declare success.
     */
    public abstract boolean succeededOptimally();

    /**
     * Simple accessor for checking whether call succeeded as well as it
     * could; meaning that no further calls should be made, even if
     * more nodes were available.
     */
    public abstract boolean succeededMaximally();
    
    public abstract int getFailCount();
    public abstract int getIgnoreCount();

    public abstract int getSuccessCount();

    public abstract Iterable<NodeFailure> getFailures();
    public abstract Iterable<ServerNodeState> getIgnoredServers();

    public abstract NodeFailure getFirstFail();
}
