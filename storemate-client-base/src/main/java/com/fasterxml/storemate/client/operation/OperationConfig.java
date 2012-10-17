package com.fasterxml.storemate.client.operation;

import com.fasterxml.storemate.client.call.CallConfig;

public class OperationConfig
{
    private final static CallConfig DEFAULT_CALL_CONFIG = new CallConfig();
    
    // // // High-level operation setting defaults
    
    /**
     * By default we only need one successful store/delete operation requests
     * for the operation to succeed.
     */
    public final static int DEFAULT_MIN_OKS_PER_OPERATION = 1;

    /**
     * By default we will try to call up to 2 stores (for PUT, DELETE) to
     * keep things highly consistent.
     */
    public final static int DEFAULT_OPTIMAL_OKS_PER_OPERATION = 2;

    /**
     * By default we will try to call up to 3 stores (for PUT, DELETE)
     * to allow new nodes to catch up when we are resizing the cluster
     * or swapping out nodes; during transition we will usually have
     * one more node
     *<p>
     * Note: may be set higher if need be; only matters in cases where
     * we actually do have more nodes available; so setting to, say, 100,
     * should seldom hurt.
     */
    public final static int DEFAULT_MAX_OKS = 4;
    
    // PUTs more expensive as operations as well
    public final static long DEFAULT_PUT_OPERATION_TIMEOUT_MSECS = 30000L;

    // but at operation level, shouldn't bail out too early with GETs either
    public final static long DEFAULT_GET_OPERATION_TIMEOUT_MSECS = 15000L;

    public final static long DEFAULT_DELETE_OPERATION_TIMEOUT_MSECS = 20000L;

    // // // Per-call settings
    
    protected final CallConfig _callConfig;
    
    // // // High-level operation settings, OK calls to require/try
    
    protected final int _minOksToSucceed;

    protected final int _optimalOks;

    protected final int _maxOks;
    
    // // // High-level operation settings, timeouts
    
    protected final long _putOperationTimeoutMsecs;

    protected final long _getOperationTimeoutMsecs;

    protected final long _deleteOperationTimeoutMsecs;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Instance creation, building
    ///////////////////////////////////////////////////////////////////////
     */
    
    public OperationConfig() {
        this(DEFAULT_CALL_CONFIG,
                DEFAULT_MIN_OKS_PER_OPERATION,
                DEFAULT_OPTIMAL_OKS_PER_OPERATION,
                DEFAULT_MAX_OKS,

                DEFAULT_PUT_OPERATION_TIMEOUT_MSECS,
                DEFAULT_GET_OPERATION_TIMEOUT_MSECS,
                DEFAULT_DELETE_OPERATION_TIMEOUT_MSECS
        );
    }

    public OperationConfig(CallConfig callConfig,
            int minOks, int optimalOks, int maxOks,
            long put, long get, long delete)
    {
        _callConfig = callConfig;
        
        _minOksToSucceed = minOks;
        _optimalOks = optimalOks;
        _maxOks = maxOks;
        
        _putOperationTimeoutMsecs = put;
        _getOperationTimeoutMsecs = get;
        _deleteOperationTimeoutMsecs = delete;
    }

    public OperationConfig withCallConfig(CallConfig cc) {
        return (_callConfig == cc) ? this : new OperationConfig(cc,
                _minOksToSucceed, _optimalOks, _maxOks,
                _putOperationTimeoutMsecs, _getOperationTimeoutMsecs, _deleteOperationTimeoutMsecs
                );
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Public API
    ///////////////////////////////////////////////////////////////////////
     */

    public CallConfig getCallConfig() { return _callConfig; }
    
    /**
     * Number of individual calls that must succeed for the write operation
     * (that is, not used for read ops like GET or HEAD)
     * to be considered "partially successful" (i.e. acceptable but not optimal).
     */
    public int getMinimalOksToSucceed() { return _minOksToSucceed; }

    /**
     * Number of individual calls that must succeed for the write operation
     * (that is, not used for read ops like GET or HEAD)
     * to be considered "fully successful".
     * Used for determining whether to try making more calls after each
     * individual success response received.
     */
    public int getOptimalOks() { return _optimalOks; }

    /**
     * Number of maximum number of successfull write calls
     * (that is, not used for read ops like GET or HEAD)
     * to try, in case we have more than usual number of replicas:
     * typically only matters during transitions when new nodes
     * are being added.
     *<p>
     * It is common to define this to be 1 or 2 higher than
     * {@link #getOptimalOks()}
     */
    public int getMaxOks() { return _maxOks; }
    
    public long getPutOperationTimeoutMsecs() { return _putOperationTimeoutMsecs; }
    public long getGetOperationTimeoutMsecs() { return _getOperationTimeoutMsecs; }
    public long getDeleteOperationTimeoutMsecs() { return _deleteOperationTimeoutMsecs; }
    
}
