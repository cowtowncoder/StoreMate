package com.fasterxml.storemate.client.call;

import com.fasterxml.storemate.client.CallFailure;


public abstract class HeadCallResult
{
    protected final int _status;

    protected final CallFailure _fail;
    
    protected final long _contentLength;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, initialization
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected HeadCallResult(int status, long contentLength)
    {
        _status = status;
        _contentLength = contentLength;
        _fail = null;
    }

    protected HeadCallResult(CallFailure fail)
    {
        _status = fail.getStatusCode();
        _contentLength = -1;
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
    public long getContentLength() { return _contentLength; }
    public boolean hasContentLength() { return _contentLength >= 0L; }
}
