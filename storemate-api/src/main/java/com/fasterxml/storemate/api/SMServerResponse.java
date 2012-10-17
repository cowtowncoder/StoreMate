package com.fasterxml.storemate.api;

import com.fasterxml.storemate.api.HTTPConstants;
import com.fasterxml.storemate.api.SMServerResponse;
import com.fasterxml.storemate.api.StreamingResponseContent;


/**
 * Interface class that defines interface of (HTTP) Responses
 * server returns to caller.
 *<p>
 * Separated out to allow handlers to operate independent of the
 * container like Servlet or JAX-RS container.
 */
public abstract class SMServerResponse
{
    protected Object _entity;

    protected StreamingResponseContent _streamingContent;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Low-level response building
    ///////////////////////////////////////////////////////////////////////
     */
    
    public abstract SMServerResponse set(int code, Object entity);

    public abstract SMServerResponse setStatus(int code);
    
    public abstract SMServerResponse addHeader(String key, String value);

    public abstract SMServerResponse addHeader(String key, int value);

    public abstract SMServerResponse addHeader(String key, long value);
    
    /**
     * Method for specifying POJO to serialize as content of response;
     * either as streaming content (if entity is of type
     * {@link StreamingResponseContent}); or as something to serialize
     * using default serialization mechanism (usually JSON).
     */
    @SuppressWarnings("unchecked")
    public final <T extends SMServerResponse> T setEntity(Object e)
    {
        if (e instanceof StreamingResponseContent) {
            _entity = null;
            _streamingContent = (StreamingResponseContent) e;
        } else {
            _entity = e;
            _streamingContent = null;
        }
        return (T) this;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Basic accessors
    ///////////////////////////////////////////////////////////////////////
     */

    public abstract int getStatus();

    public final boolean isError() { return getStatus() >= 300; }

    public final boolean hasEntity() { return _entity != null; }
    public final boolean hasStreamingContent() { return _streamingContent != null; }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // High(er)-level response building; semantic headers
    ///////////////////////////////////////////////////////////////////////
     */

    public abstract SMServerResponse setContentType(String contentType);

    public SMServerResponse setContentTypeJson() {
        return setContentType("application/json");
    }
    
    public final SMServerResponse setBodyCompression(String type) {
        return addHeader(HTTPConstants.HTTP_HEADER_COMPRESSION, type);
    }
    
    public abstract SMServerResponse setContentLength(long length);

    /*
    ///////////////////////////////////////////////////////////////////////
    // High(er)-level response building; ok cases
    ///////////////////////////////////////////////////////////////////////
     */
    
    public final SMServerResponse ok() {
        return setStatus(200);
    }
    
    public final SMServerResponse ok(Object entity) {
        return setEntity(entity);
    }

    public final SMServerResponse noContent() {
        return setStatus(204);
    }

    public final SMServerResponse partialContent(Object entity, String rangeDesc) {
        // 206 means "partial content"
        return set(HTTPConstants.HTTP_STATUS_OK_PARTIAL, entity)
                .addHeader(HTTPConstants.HTTP_HEADER_RANGE_FOR_RESPONSE, rangeDesc);
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // High(er)-level response building; error cases
    ///////////////////////////////////////////////////////////////////////
     */
    
    public final SMServerResponse badRange(Object entity) {
        // 416 is used for invalid Range requests
        return set(416, entity);
    }

    public final SMServerResponse badRequest(Object entity) {
        return set(400, entity);
    }

    public final SMServerResponse conflict(Object entity) {
        return set(409, entity);
    }

    public final SMServerResponse gone(Object entity) {
        return set(410, entity);
    }
    
    
    public final SMServerResponse internalError(Object entity) {
        return set(500, entity);
    }

    public final SMServerResponse notFound(Object entity) {
        return set(404, entity);
    }
}
