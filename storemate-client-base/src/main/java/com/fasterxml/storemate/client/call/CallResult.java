package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.client.CallFailure;

public abstract class CallResult
{
    protected final int _status;

    protected final CallFailure _fail;

    protected CallResult(int statusCode) {
        this(statusCode, null);
    }

    protected CallResult(CallFailure fail) {
        this(fail.getStatusCode(), fail);
    }
    
    protected CallResult(int statusCode, CallFailure fail)
    {
        _status = statusCode;
        _fail = fail;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Public API
    ///////////////////////////////////////////////////////////////////////
     */
    
    public int getStatus() { return _status; }

    public abstract String getHeaderValue(String key);
    
    public boolean failed() { return _fail != null; }
    public boolean succeeded() { return !failed(); }

    public CallFailure getFailure() { return _fail; }
}
