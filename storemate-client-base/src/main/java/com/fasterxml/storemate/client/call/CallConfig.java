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

    /**
     * There is certain minimum practical per-call timeout beyond which we should
     * not reduce timeout (but probably fail the call right away)
     */
    public final static long MIN_TIMEOUT_MSECS = 50L;

    /**
     * By default, let's just get first 500 ASCII characters (about 6 rows) from error response;
     * might help figure out what is going wrong...
     */
    private final static int DEFAULT_MAX_EXCERPT_LENGTH = 500;

    
    // // // Low-level single-call timeouts
    
    protected final long _connectTimeoutMsecs;

    protected final long _putCallTimeoutMsecs;

    protected final long _getCallTimeoutMsecs;

    protected final long _deleteCallTimeoutMsecs;

    // // // Other settings

    protected final int _maxExcerptLength;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Instance creation, building
    ///////////////////////////////////////////////////////////////////////
     */
    
    public CallConfig() {
        this(DEFAULT_CONNECT_TIMEOUT_MSECS,
                DEFAULT_PUT_CALL_TIMEOUT_MSECS,
                DEFAULT_GET_CALL_TIMEOUT_MSECS,
                DEFAULT_DELETE_CALL_TIMEOUT_MSECS,
                DEFAULT_MAX_EXCERPT_LENGTH);
    }

    public CallConfig(long connect,
            long put, long get, long delete,
            int maxExcerptLength)
    {
        _connectTimeoutMsecs = connect;
        _putCallTimeoutMsecs = put;
        _getCallTimeoutMsecs = get;
        _deleteCallTimeoutMsecs = delete;
        _maxExcerptLength = maxExcerptLength;
    }

    public CallConfig withConnectTimeout(long t) {
        return (t == _connectTimeoutMsecs) ? this :
            new CallConfig(t,
                    _putCallTimeoutMsecs, _getCallTimeoutMsecs, _deleteCallTimeoutMsecs,
                    _maxExcerptLength);
    }

    public CallConfig withPutTimeout(long t) {
        return (t == _putCallTimeoutMsecs) ? this :
            new CallConfig(_connectTimeoutMsecs, 
                    t, _getCallTimeoutMsecs, _deleteCallTimeoutMsecs,
                    _maxExcerptLength);
    }

    public CallConfig withGetTimeout(long t) {
        return (t == _getCallTimeoutMsecs) ? this :
            new CallConfig(_connectTimeoutMsecs, 
                    _putCallTimeoutMsecs, t, _deleteCallTimeoutMsecs,
                    _maxExcerptLength);
    }

    public CallConfig withDeleteTimeout(long t) {
        return (t == _deleteCallTimeoutMsecs) ? this :
            new CallConfig(_connectTimeoutMsecs, 
                    _putCallTimeoutMsecs, _getCallTimeoutMsecs, t,
                    _maxExcerptLength);
    }

    public CallConfig withMaxExcerptLength(int t) {
        return (t == _maxExcerptLength) ? this :
            new CallConfig(_connectTimeoutMsecs, 
                    _putCallTimeoutMsecs, _getCallTimeoutMsecs, _deleteCallTimeoutMsecs,
                    t);
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

    public int getMaxExcerptLength() {
        return _maxExcerptLength;
    }
    
    // NOTE: not configurable currently, can change should this change
    public long getMinimumTimeoutMsecs() {
        return MIN_TIMEOUT_MSECS;
    }
}
