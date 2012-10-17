package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.client.CallFailure;

public abstract class GetCallResult<T>
    extends CallResult
{
    protected final T _result;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, initialization
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected GetCallResult(int status, T result)
    {
        super(status);
        _result = result;
    }

    protected GetCallResult(CallFailure fail)
    {
        super(fail);
        _result = null;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // CallResult impl
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public abstract String getHeaderValue(String key);
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Extended API
    ///////////////////////////////////////////////////////////////////////
     */
    
    public T getResult() { return _result; }
}
