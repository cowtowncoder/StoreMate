package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.client.CallFailure;


public abstract class GetCallResult<T>
{
    protected final int _status;

    protected final CallFailure _fail;
    
    protected final T _result;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, initialization
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected GetCallResult(int status, T result)
    {
        _status = status;
        _result = result;
        _fail = null;
    }

    protected GetCallResult(CallFailure fail)
    {
        _status = fail.getStatusCode();
        _result = null;
        _fail = fail;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Accessors
    ///////////////////////////////////////////////////////////////////////
     */
    
    public int getStatus() { return _status; }

    public abstract String getHeaderValue(String key);
    
    public boolean failed() { return _fail != null; }
    public boolean succeeded() { return !failed(); }

    public CallFailure getFailure() { return _fail; }
    public T getResult() { return _result; }
}
