package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.client.CallFailure;

public abstract class HeadCallResult
    extends CallResult
{
    protected final long _contentLength;

    /*
    /**********************************************************************
    /* Construction, initialization
    /**********************************************************************
     */
    
    protected HeadCallResult(int status, long contentLength)
    {
        super(status);
        _contentLength = contentLength;
    }

    protected HeadCallResult(CallFailure fail)
    {
        super(fail);
        _contentLength = -1;
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
    
    public long getContentLength() { return _contentLength; }
    public boolean hasContentLength() { return _contentLength >= 0L; }
}
