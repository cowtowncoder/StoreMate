package com.force.vagabond.client;

/**
 * Class that defines interface that physical requests to be sent
 * to (HTTP) service implement.
 *<p>
 * Separated out mostly to allow use of different HTTP Client
 * implementations.
 */
public abstract class VClientRequest
{
    public abstract String getPath();
    
    public abstract String getQueryParameter(String key);

    public abstract String getHeader(String key);

    public abstract String findClientId();
    /*
    public String findClientId() {
        return getQueryParameter(HTTPConstants.HTTP_QUERY_PARAM_CLIENT_ID);
    }
     */
}
