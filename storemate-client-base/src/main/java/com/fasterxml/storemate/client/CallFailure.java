package com.fasterxml.storemate.client;

import com.fasterxml.storemate.shared.HTTPConstants;

/**
 * Container for details of a single failed call to a server.
 */
public class CallFailure
{
    protected final ServerNode _server;

    protected final long _callTime;
    
    protected final int _statusCode;
    
    protected final int _timeTakenMsecs;

    protected final Throwable _error;

    /**
     * Specific error message, either constructed explicitly, or
     * extracted from JSON response.
     */
    protected final String _errorMessage;

    /**
     * Excerpt of the underlying error message.
     */
    protected final byte[] _rawResponse;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */

    public CallFailure(ServerNode server, int statusCode,
            long callTime, long endTime,
            String errorMsg, byte[] responseExcerpt)
    {
        _server = server;
        _statusCode = statusCode;
        _callTime = callTime;
        _timeTakenMsecs = (int) (endTime - callTime);
        _error = null;
        _errorMessage = errorMsg;
        _rawResponse = responseExcerpt;
    }

    public CallFailure(ServerNode server, int statusCode,
            long callTime, long endTime,
            Throwable error, byte[] responseExcerpt)
    {
        _server = server;
        _statusCode = statusCode;
        _callTime = callTime;
        _timeTakenMsecs = (int) (endTime - callTime);
        _error = error;
        _errorMessage = (error == null) ? "Unknown error" : error.toString();
        _rawResponse = responseExcerpt;
    }
    
    /**
     * Factory method called to indicate that a failure was due to timeout; either
     * for actual timed out call, or not being able to make a call due to operation
     * time out
     */
    public static CallFailure timeout(ServerNode server, long callTime, long endTime) {
        return new CallFailure(server, HTTPConstants.HTTP_STATUS_TIMEOUT_ON_READ,
                callTime, endTime, "timeout after "+ (endTime - callTime) + " msecs",
                null);
    }

    /**
     * Factory method for general "not sure what or why failed" failure
     */
    public static CallFailure general(ServerNode server, int statusCode, long callTime,
            long endTime, String msg) {
        return new CallFailure(server, statusCode, callTime, endTime, msg, null);
    }

    /**
     * Factory method for "could not parse response" failure.
     */
    public static CallFailure formatException(ServerNode server, int statusCode, long callTime,
            long endTime, String msg) {
        return new CallFailure(server, statusCode, callTime, endTime, msg, null);
    }
    
    /**
     * Factory method for general I/O failure, caused by HTTP request or response
     * processing.
     */
    public static CallFailure ioProblem(ServerNode server, int statusCode, long callTime,
            long endTime, String msg, Exception e)
    {
        if (e != null) {
            Throwable cause = _peel(e);
            msg += " (exception of type "+cause.getClass().getName()+": "+cause.getMessage()+")";
        }
        return new CallFailure(server, statusCode, callTime, endTime, msg, null);
    }
    
    /**
     * Factory method for "internal failure"; case where something in our code
     * threw an exception causing individual call to fail, and we have an
     * exception indicating what happened.
     */
    public static CallFailure internal(ServerNode server, long callTime,
            long endTime, Throwable cause) {
        cause = _peel(cause);
        return new CallFailure(server, HTTPConstants.HTTP_STATUS_CUSTOM_FAIL_THROWABLE,
                callTime, endTime, cause, null);
    }

    /**
     * Factory method for "internal failure"; case where something in our code
     * threw an exception causing individual call to fail, but we did not
     * get an exception.
     */
    public static CallFailure internal(ServerNode server, long callTime,
            long endTime, String msg) {
        return new CallFailure(server, HTTPConstants.HTTP_STATUS_CUSTOM_FAIL_MESSAGE,
                callTime, endTime, msg, null);
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Simple accessors
    ///////////////////////////////////////////////////////////////////////
     */
    
    public ServerNode getServer() { return _server; }
    public int getStatusCode() { return _statusCode; }

    /**
     * Accessor that can be used to find out client-side {@link Throwable}
     * that caused call to fail, if failure was due to a {@link Throwable}
     * being thrown.
     */
    public Throwable getCause() { return _error; }
    
    /**
     * Accessor for getting timestamp of when request attempt was made ("start time")
     */
    public long getCallTime() { return _callTime; }

    public long getEndTime() {
        if (_timeTakenMsecs > 0) {
            return _callTime + _timeTakenMsecs;
        }
        return _callTime;
    }
    
    public int getTimeTakenMsecs() { return _timeTakenMsecs; }

    /**
     * Accessor for explicitly provided failure message; either from code
     * that knows what failed, or from response message successfully parsed.
     */
    public String getErrorMessage() {
        return (_errorMessage == null) ? "N/A" : _errorMessage;
    }

    /**
     * Accessor for getting actual underlying error message (or, if very long,
     * extract of first N bytes). Null for failures that did not get a response.
     */
    public String getResponseExtract()
    {
        if (_rawResponse == null) {
            return null;
        }
        if (_rawResponse.length == 0) {
            return "";
        }
        try {
            return new String(_rawResponse, "ISO-8859-1");
        } catch (Exception e) {
            return "DECODING ERROR for "+_rawResponse.length+" bytes: "+e.getMessage();
        }
    }

    /**
     * Accessor that can be called to determine whether failure is of transient type,
     * and might be resolved by retrying after a brief delay.
     */
    public boolean isRetriable()
    {
        // We can actually just retry 5xx codes (which includes timeouts)
        return (_statusCode >= 500) && (_statusCode < 600);
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Overrides
    ///////////////////////////////////////////////////////////////////////
     */
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(100);
        sb.append("[Call Failure to ").append(_server.getAddress())
            .append(": status=").append(_statusCode)
            .append(", errorMessage='").append(_errorMessage).append("'");
        String resp = getResponseExtract();
        if (resp != null) {
            sb.append(", response='").append(resp).append("'");
        }
        sb.append(']');
        return sb.toString();
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Helper methods
    ///////////////////////////////////////////////////////////////////////
     */
    
    private static Throwable _peel(Throwable t)
    {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }
}
