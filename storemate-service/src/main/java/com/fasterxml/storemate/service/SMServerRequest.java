package com.fasterxml.storemate.service;

import com.fasterxml.storemate.shared.ByteRange;

/**
 * Interface class that defines abstraction implemented by classes that
 * enclose details of a (HTTP) request that server receives.
 *<p>
 * Separated out to allow handlers to operate independent of the
 * container like Servlet or JAX-RS container.
 */
public abstract class SMServerRequest
{
    public abstract String getPath();
    
    public abstract String getQueryParameter(String key);

    public abstract String getHeader(String key);

    public ByteRange findByteRange()
    {
        String rangeStr = getHeader(HTTPConstants.HTTP_HEADER_RANGE_FOR_REQUEST);
        return (rangeStr == null) ? null : ByteRange.valueOf(rangeStr);
    }
}

