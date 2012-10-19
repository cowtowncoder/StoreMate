package com.fasterxml.storemate.client.operation;

import java.util.Collection;
import java.util.LinkedList;

import com.fasterxml.storemate.client.cluster.ServerNodeState;

/**
 * Intermediate base class to simplify actual result implementations.
 */
public abstract class OperationResultImpl<T extends OperationResult<T>>
    extends OperationResult<T>
{
    protected final OperationConfig _config;

    /**
     * Information on servers for which at least one call was made, but none of calls
     * succeeded.
     */
    protected final Collection<NodeFailure> _failed;

    /**
     * Set of servers that were not contacted; either due to timeout or because
     * no further calls were necessary (disabled nodes, for example, are only
     * called if absolutely necessary)
     */
    protected final Collection<ServerNodeState> _ignored;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, initialization
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected OperationResultImpl(OperationConfig config)
    {
        _config = config;
        _failed = new LinkedList<NodeFailure>();
        _ignored = new LinkedList<ServerNodeState>();
    }


    // Ugly side of generics... needing to cast "this", from base class.
    @SuppressWarnings("unchecked")
    protected final T _this() {
        return (T) this;
    }

    public T addFailed(NodeFailure fail) {
        _failed.add(fail);
        return _this();
    }

    public T addFailed(Collection<NodeFailure> fails) {
        if (fails != null) {
            for (NodeFailure fail : fails) {
                _failed.add(fail);
            }
        }
        return _this();
    }
    
    public T addIgnored(ServerNodeState server) {
        _ignored.add(server);
        return _this();
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Accessors
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public OperationConfig getConfig() { return _config; }

    /**
     * Simple accessor for checking whether call succeeded to minimum degree
     * required, or not. This means that we had at least minimal required number
     * of succesful individual calls.
     */
    @Override
    public boolean succeededMinimally() {
        return getSuccessCount() >= _config.getMinimalOksToSucceed();
    }

    /**
     * Simple accessor for checking whether call succeeded well
     * enough that we may consider it full success.
     * We may either choose to do more updates (if nodes are available);
     * up to {@link #succeededMaximally()} level; or just return
     * and declare success.
     */
    @Override
    public boolean succeededOptimally() {
        return getSuccessCount() >= _config.getOptimalOks();
    }

    /**
     * Simple accessor for checking whether call succeeded as well as it
     * could; meaning that no further calls should be made, even if
     * more nodes were available.
     */
    @Override
    public boolean succeededMaximally() {
        return getSuccessCount() >= _config.getMaxOks();
    }
    
    @Override
    public int getFailCount() { return _failed.size(); }

    @Override
    public abstract int getSuccessCount();
    
    @Override
    public int getIgnoreCount() { return _ignored.size(); }

    @Override
    public Iterable<NodeFailure> getFailures() { return _failed; }
    @Override
    public Iterable<ServerNodeState> getIgnoredServers() { return _ignored; }

    @Override
    public NodeFailure getFirstFail() {
        return _failed.isEmpty() ? null : _failed.iterator().next();
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Overrides
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[Result: ");
        sb.append("succeed: ").append(getSuccessCount());
        sb.append(", failed: ").append(getFailCount());
        sb.append(", ignored: ").append(getIgnoreCount());
        _addExtraInfo(sb);
        sb.append("]");
        return sb.toString();
    }

    /**
     * Overridable method to augment 'toString()' results
     */
    protected void _addExtraInfo(StringBuilder sb) { }
}
