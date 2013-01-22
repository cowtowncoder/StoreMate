package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.client.CallFailure;

public abstract class EntryListResult<T>
    extends CallResult
{
    protected final T _result;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */
    
    public EntryListResult(int status, T result)
    {
        super(status);
        _result = result;
    }

    public EntryListResult(CallFailure fail)
    {
        super(fail);
        _result = null;
    }

    /*
    /**********************************************************************
    /* CallResult impl
    /**********************************************************************
     */

    @Override
    public abstract String getHeaderValue(String key);
    
    /*
    /**********************************************************************
    /* Extended API
    /**********************************************************************
     */
    
    public T getResult() { return _result; }
}
