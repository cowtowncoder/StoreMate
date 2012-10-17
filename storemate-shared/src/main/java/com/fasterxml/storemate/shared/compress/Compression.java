package com.fasterxml.storemate.shared.compress;

public enum Compression
{
    /**
     * Indicates case where no compression algorithm is used
     */
    NONE('N', "identity", 0),

    /**
     * Indicates use of LZF compression (fast, modest compression)
     */
    LZF('L', "lzf", 1),

    /**
     * Indicates use of basic deflate compression, no header;
     * 'Z' from zip (although technically closer to gzip)
     */
    GZIP('Z', "gzip", 2)
    ;
    
    private final char _char;

    private final int _index;

    private final String _contentEncoding;
    
    private Compression(char c, String contentEnc, int index)
    {
        _char = c;
        _index = index;
        _contentEncoding = contentEnc;
    }

    public char asChar() { return _char; }
    public int asIndex() { return _index; }

    public String asContentEncoding()
    {
        // for NONE we could also use "identity" but...
        /*
        if (this == NONE) {
            return null;
        }
        */
        return _contentEncoding;
    }

    @Override
    public String toString() {
        return (this == NONE) ? "none" : _contentEncoding;
    }
    
    /**
     * Helper method that can be called to see if this Compression
     * method is one of acceptable encodings that client has
     * listed.
     */
    public boolean isAcceptable(String acceptableEncodings)
    {
        if (acceptableEncodings == null) {
            return false;
        }
        // crude, but functional due to small number of legal values:
        return acceptableEncodings.indexOf(_contentEncoding) >= 0;
    }

    public static Compression from(String str)
    {
    	if (str == null) return null;
    	str = str.trim();
    	if (GZIP._contentEncoding.equals(str)) {
    		return GZIP;
    	}
    	if (LZF._contentEncoding.equals(str)) {
    		return LZF;
    	}
    	if (NONE._contentEncoding.equals(str)) {
    		return NONE;
    	}
    	return null;
    }
    
    /*
    public static Compression valueOf(byte b, boolean errorForUnknown) {
        return valueOf((char) b, errorForUnknown);
    }
    
    @Deprecated
    public static Compression valueOf(char c, boolean errorForUnknown)
    {
        if (c == '\0') return NONE;
        for (Compression comp : values()) {
            if (comp.asChar() == c) {
                return comp;
            }
        }
        if (errorForUnknown) {
            throw new IllegalArgumentException("Unrecognized compression value: 0x"
                    +Integer.toHexString(c)+"");
        }
        return null;
    }
    */

    public static Compression forIndex(int index, boolean errorForUnknown)
    {
        for (Compression comp : values()) {
            if (comp.asIndex() == index) {
                return comp;
            }
        }
        if (errorForUnknown) {
            throw new IllegalArgumentException("Unrecognized compression value: 0x"
                    +Integer.toHexString(index)+" (currently only values 0 - 2 supported)");
        }
        return null;
    }

    /**
     * Factory method used for finding match for given HTTP
     * Content-Encoding value, if possible.
     * Will return null if no match done.
     */
    public static Compression forContentEncoding(String contentEncoding)
    {
        if (contentEncoding == null) return null;
        contentEncoding = contentEncoding.trim();
        // not the cleanest, but should do for now:
        if (GZIP._contentEncoding.equals((contentEncoding))) {
            return GZIP;
        }
        if (LZF._contentEncoding.equals((contentEncoding))) {
            return LZF;
        }
        // can leave 'NONE' to default handling:
        return null;
    }
}
