package com.fasterxml.storemate.shared;

/**
 * Helper class used for dealing with HTTP "Range" and "Content-Range" headers.
 */
public class ByteRange
{
    private final static String PREFIX_BYTES = "bytes";
    
    protected final String _source;
    
    protected final long _start;
    protected final long _end;
    protected final long _totalLength;

    /**
     * Constructor used when programmatically creating instances when sending
     * Range requests to server.
     */
    public ByteRange(long start, long length)
    {
    	_start = start;
    	// end is last included byte, hence -1
    	_end = start + length - 1;
    	_source = null;
    	_totalLength = -1;
    }
    
    protected ByteRange(long start, long end, long totalLength, String src)
    {
        _start = start;
        _end = end;
        _source = src;
        _totalLength = totalLength;
    }

    /**
     * Method called to create a new instance with known total
     * length; which may be needed to resolve suffix ranges
     * (negative end)
     */
    public ByteRange resolveWithTotalLength(long totalLength)
    {
    	long start = _start;
    	long end = _end;

        // missing end?
        if (end < 0) {
            end = totalLength-1;
        }
        // suffix?
        if (start < 0) { // suffix, yup; must modify
            // since start is offset from end (and negative), works with addition
            // but we must consider possibility it might go beyond start, so:
            start = Math.max(0, totalLength+start);
        }
        return new ByteRange(start, end, totalLength, _source);
    }

    public static ByteRange valueOf(String external)
        throws IllegalArgumentException
    {
        if (external == null) return null;
        external = external.trim();
        if (external.length() == 0) return null;
        final String rangeDesc = external;

        // Note: code is verbose -- regexp would be way shorter -- but goal is to give meaningful errors
        
        // ok: must start with "bytes"
        if (!external.startsWith(PREFIX_BYTES)) {
            throw new IllegalArgumentException("range does note start with 'bytes='");
        }
        external = external.substring(PREFIX_BYTES.length()).trim();
        if (external.startsWith("=")) {
            external = external.substring(1).trim();
        }

        // One more thing: we do NOT support multiple ranges
        if (external.indexOf(',') >= 0) {
            throw new IllegalArgumentException("multiple ranges not supported");
        }

        long totalLength;

        // Should we accept total length suffix? It's only sent for response... but
        int ix;
        ix = external.indexOf('/');
        if (ix < 0) {
        	totalLength = -1;
        } else {
        	String str = external.substring(ix+1);
        	external = external.substring(0, ix).trim();
        	if ("*".equals(str)) {
        		totalLength = -1L;
        	} else {
	        	try {
	        		totalLength = Long.parseLong(str);
	        	} catch (NumberFormatException e) {
	                throw new IllegalArgumentException("invalid instance-length suffix for range: '"+str+"'");
	        	}
        	}
        }
        
        // and then either suffix entry or range
        
        ix = external.indexOf('-');
        if (ix < 0) {
            throw new IllegalArgumentException("no hyphen found");
        }
        if (ix == 0) { // suffix entry
            // suffix range, yay; parse as negative number
            return new ByteRange(parseLong(external), -1, -1, rangeDesc);
        }
        // actual range
        long start = parseLong(external.substring(0,ix));
        String num = external.substring(ix+1).trim();
        long end;
        if (num.length() == 0) { // missing end range -- fine, use -1 to indicate 'till the end'
            end = -1;
        } else {
            end = parseLong(num);
            if (end < start) {
                throw new IllegalArgumentException("range end can not be less than start");
            }
        }
        return new ByteRange(start, end, totalLength, rangeDesc);
    }

    private final static long parseLong(String num)
    {
        try {
            return Long.parseLong(num);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid number '"+num+"'");
        }
    }
    
    public long getStart() { return _start; }
    public long getEnd() { return _end; }
    public long getTotalLength() { return _totalLength; }

    public long calculateLength() {
        return (_end - _start) + 1;
    }

    /**
     * Method to use for creating value suitable for sending as
     * request header (Range). Does not include "total length" part.
     */
    public String asRequestHeader()
    {
    	StringBuilder sb = new StringBuilder(30)
            .append("bytes ").append(_start)
            .append('-').append(_end);
    	return sb.toString();
    }

    /**
     * Method to use for creating value suitable for sending as
     * response header (Content-Range). Does include "total length" part.
     */
    public String asResponseHeader()
    {
    	StringBuilder sb = new StringBuilder(30)
            .append("bytes ").append(_start)
            .append('-').append(_end);
    	if (_totalLength < 0) {
    		sb.append("/*");
    	} else {
    		sb.append('/').append(_totalLength);
    	}
    	return sb.toString();
    }
    
    @Override
    public String toString() {
        if (_source != null) {
            return _source;
        }
        return asResponseHeader();
    }
}
