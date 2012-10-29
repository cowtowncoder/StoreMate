package com.fasterxml.storemate.shared;

/**
 * Set of standard HTTP constants that we tend to need in various
 * places. Although conceptually separate seems useful to just
 * share them via one "constant class".
 */
public abstract class HTTPConstants
{
    /*
    /**********************************************************************
    /* Standard HTTP Headers
    /**********************************************************************
     */

    /**
     * Standard HTTP header indicating type of content (payload) of the
     * response/request.
     */
    public final static String HTTP_HEADER_CONTENT_TYPE = "Content-Type";
    
    /**
     * Standard HTTP header indicating compression that Entity (payload) uses,
     * if any.
     */
    public final static String HTTP_HEADER_COMPRESSION = "Content-Encoding";

    /**
     * Standard HTTP header indicating content type(s) caller accepts
     */
    public final static String HTTP_HEADER_ACCEPT = "Accept";
    
    /**
     * Standard HTTP header indicating compression methods client accepts,
     * if any.
     */
    public final static String HTTP_HEADER_ACCEPT_COMPRESSION = "Accept-Encoding";

    /**
     * Standard HTTP header that indicates length of entity in bytes, if length
     * is known; missing or -1 if not known.
     */
    public final static String HTTP_HEADER_CONTENT_LENGTH = "Content-Length";
    
    public final static String HTTP_HEADER_RANGE_FOR_REQUEST = "Range";
    
    public final static String HTTP_HEADER_RANGE_FOR_RESPONSE = "Content-Range";

    /*
    /**********************************************************************
    /* Custom HTTP Headers
    /**********************************************************************
     */
    
    /**
     * Name of custom HTTP header we use to indicate the latest
     * available cluster state update from given server node.
     *<p>
     * Whether to use "X-" prefix or not seems in dispute; can change
     * if need be.
     */
    public final static String CUSTOM_HTTP_HEADER_LAST_CLUSTER_UPDATE = "X-TempStore-ClusterUpdate";

    /**
     * In case of PUT that does not provide checksum as argument, server may
     * return checksum upon successful call. This can be used for further
     * calls by client.
     */
    public final static String CUSTOM_HTTP_HEADER_CHECKSUM = "X-TempStore-Checksum";

    /*
    ///////////////////////////////////////////////////////////////////////
    // Query parameters, StoreMate-specific
    ///////////////////////////////////////////////////////////////////////
     */

    public final static String HTTP_QUERY_PARAM_CLIENT_ID = "clientId";

    public final static String HTTP_QUERY_PARAM_CHECKSUM = "checksum";

    public final static String HTTP_QUERY_PARAM_MIN_SINCE_ACCESS_TTL = "minSinceAccessTTL";
    public final static String HTTP_QUERY_PARAM_MAX_TTL = "maxTTL";
    
    /**
     * Query parameter used for defining timestamp after which (inclusive) entries are
     * to be returned, as determine by their insertion time.
     */
    public final static String HTTP_QUERY_PARAM_SINCE = "since";
    
    public final static String HTTP_QUERY_PARAM_KEYRANGE_START = "keyRangeStart";

    public final static String HTTP_QUERY_PARAM_KEYRANGE_LENGTH = "keyRangeLength";
    
    /*
    /**********************************************************************
    /* HTTP Response codes, standard
    /**********************************************************************
     */

    public final static int HTTP_STATUS_OK = 200;

    public final static int HTTP_STATUS_OK_PARTIAL = 206;

    public final static int HTTP_STATUS_NOT_FOUND = 404;

    /*
    /**********************************************************************
    /* HTTP Response codes, custom
    /**********************************************************************
     */

    /* Response code used when the request timed out; as per docs, while
     * not a formally standardized code, is actually used. And is considered
     * retriable (as 5xx code) which is why we choose it.
     */
    public final static int HTTP_STATUS_TIMEOUT_ON_READ = 598;

    public final static int HTTP_STATUS_CUSTOM_FAIL_THROWABLE = -2;

    public final static int HTTP_STATUS_CUSTOM_FAIL_MESSAGE = -3;
    
    /*
    /**********************************************************************
    /* Content types
    /**********************************************************************
     */

    public final static String CONTENT_TYPE_JSON = "application/json";
    
    public final static String CONTENT_TYPE_SMILE = "application/x-jackson-smile";
}
