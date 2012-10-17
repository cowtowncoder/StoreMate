package com.force.vagabond.client;

import com.fasterxml.storemate.api.HTTPConstants;

/**
 * Class that defines interface that physical responses (returned
 * by service, usually over HTTP) expose to backend independent
 * client functionality.
 *<p>
 * Separated out mostly to allow use of different HTTP Client
 * implementations.
 */
public abstract class VClientResponse
{
    /*
    ///////////////////////////////////////////////////////////////////////
    // Low-level response building
    ///////////////////////////////////////////////////////////////////////
     */
    
    public abstract VClientResponse set(int code, Object entity);

    public abstract VClientResponse setStatus(int code);
    
    public abstract VClientResponse addHeader(String key, String value);

    public abstract VClientResponse addHeader(String key, int value);

    public abstract VClientResponse addHeader(String key, long value);
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Basic accessors
    ///////////////////////////////////////////////////////////////////////
     */

    public abstract int getStatus();

    public final boolean isError() { return getStatus() >= 300; }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // High(er)-level response building; semantic headers
    ///////////////////////////////////////////////////////////////////////
     */

    public abstract VClientResponse setContentType(String contentType);

    public VClientResponse setContentTypeJson() {
        return setContentType("application/json");
    }
    
    public final VClientResponse setBodyCompression(String type) {
        return addHeader(HTTPConstants.HTTP_HEADER_COMPRESSION, type);
    }
    
    public final VClientResponse setContentLength(long length) {
        return addHeader(HTTPConstants.HTTP_HEADER_CONTENT_LENGTH, length);
    }
}
