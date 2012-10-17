package com.fasterxml.storemate.client.call;

public class CallConfig
{
    // // // Low-level single-call timeout defaults

    public final static long DEFAULT_CONNECT_TIMEOUT_MSECS = 600L;

    // PUTs can be more expensive; use longer timeouts
    public final static long DEFAULT_PUT_CALL_TIMEOUT_MSECS = 4000L;

    // GETs are bit cheaper, can be more aggressive
    public final static long DEFAULT_GET_CALL_TIMEOUT_MSECS = 1000L;

    // DELETEs somewhere in between
    public final static long DEFAULT_DELETE_CALL_TIMEOUT_MSECS = 2000L;

    // // // Low-level single-call timeouts
    
    protected final long _connectTimeoutMsecs;

    protected final long _putCallTimeoutMsecs;

    protected final long _getCallTimeoutMsecs;

    protected final long _deleteCallTimeoutMsecs;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Instance creation, building
    ///////////////////////////////////////////////////////////////////////
     */
    
    public CallConfig() {
        this(DEFAULT_CONNECT_TIMEOUT_MSECS,
                DEFAULT_PUT_CALL_TIMEOUT_MSECS,
                DEFAULT_GET_CALL_TIMEOUT_MSECS,
                DEFAULT_DELETE_CALL_TIMEOUT_MSECS);
    }

    public CallConfig(long connect,
            long put, long get, long delete)
    {
        _connectTimeoutMsecs = connect;
        _putCallTimeoutMsecs = put;
        _getCallTimeoutMsecs = get;
        _deleteCallTimeoutMsecs = delete;
    }

    public CallConfig withConnectTimeout(long t) {
        return (t == _connectTimeoutMsecs) ? this :
            new CallConfig(t,
                    _putCallTimeoutMsecs, _getCallTimeoutMsecs, _deleteCallTimeoutMsecs);
    }

    public CallConfig withPutTimeout(long t) {
        return (t == _putCallTimeoutMsecs) ? this :
            new CallConfig(_connectTimeoutMsecs, 
                    t, _getCallTimeoutMsecs, _deleteCallTimeoutMsecs);
    }

    public CallConfig withGetTimeout(long t) {
        return (t == _getCallTimeoutMsecs) ? this :
            new CallConfig(_connectTimeoutMsecs, 
                    _putCallTimeoutMsecs, t, _deleteCallTimeoutMsecs);
    }

    public CallConfig withDeleteTimeout(long t) {
        return (t == _deleteCallTimeoutMsecs) ? this :
            new CallConfig(_connectTimeoutMsecs, 
                    _putCallTimeoutMsecs, _getCallTimeoutMsecs, t);
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Public API
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Timeout we define for connecting to service: same for all operations, since
     * it should be less prone for per-operation changes than read timeouts.
     */
    public long getConnectTimeoutMsecs() { return _connectTimeoutMsecs; }

    public long getPutCallTimeoutMsecs() { return _putCallTimeoutMsecs; }
    public long getGetCallTimeoutMsecs() { return _getCallTimeoutMsecs; }
    public long getDeleteCallTimeoutMsecs() { return _deleteCallTimeoutMsecs; }
}
