package com.fasterxml.storemate.shared.compress;

public enum Compression
{
    /**
     * Indicates case where no compression algorithm is used
     */
    NONE('N', "identity"),

    /**
     * Indicates use of LZF compression (fast, modest compression)
     */
    LZF('L', "lzf"),

    /**
     * Indicates use of basic deflate compression, no header;
     * 'Z' from zip (although technically closer to gzip)
     */
    GZIP('Z', "gzip")
    ;
    
    private final byte _byte;
    private final char _char;
    private final int _int;

    private final String _contentEncoding;
    
    private Compression(char c, String contentEnc)
    {
        _byte = (byte) c;
        _char = c;
        _int = c;
        _contentEncoding = contentEnc;
    }

    public char asChar() { return _char; }
    public byte asByte() { return _byte; }
    public int asInt() { return _int; }

    public String asContentEncoding()
    {
        // for NONE we could also use "identity" but...
        if (this == NONE) {
            return null;
        }
        return _contentEncoding;
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
    
    public static String toString(byte comp)
    {
        Compression c = valueOf((char) comp, false);
        if (c == null) {
            return "UNKNOWN-"+(comp & 0xFF);
        }
        return c.name();
    }

    public static Compression valueOf(byte b, boolean errorForUnknown) {
        return valueOf((char) b, errorForUnknown);
    }
	
    public static Compression valueOf(char c, boolean errorForUnknown)
    {
        if (c == '\0') { // null (missing)
            return NONE;
        }
        for (Compression comp : values()) {
            if (comp.asChar() == c) {
                return comp;
            }
        }
        if (errorForUnknown) {
            throw new IllegalArgumentException("Unrecognized compression (0x"
                    +Integer.toHexString(c)+") '"+c+"'");
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
