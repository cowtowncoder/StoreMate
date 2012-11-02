package com.fasterxml.storemate.store.file;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.util.WithBytesCallback;

/**
 * Simple {@link FilenameConverter} implementation which will simply
 * take each byte, and replace all non-ASCII characters (as well as
 * a small set of "unsafe" characters like slashes) with a character
 * specified as "safe character" (by default, underscore).
 */
public class DefaultFilenameConverter extends FilenameConverter
{
    public final static char DEFAULT_SAFE_CHAR = '_';

    protected final static int[] DEFAULTS;
    static {
        int[] def = new int[256];
        // slash/backslash, double-quotes are bad... anything else?
        final String REMOVE = "/\"\\";
        
        // only allow things above Space (32), in ASCII range. And skip DEL (127)
        for (int i = 33; i < 0x7F; ++i) { // ctrl chars and Space
            char c = (char) i;
            if (REMOVE.indexOf(c) < 0) {
                def[i] = 1;
            }
        }
        DEFAULTS = def;
    }

    protected final char _safeChar;

    public DefaultFilenameConverter() {
        this(DEFAULT_SAFE_CHAR);
    }
        
    public DefaultFilenameConverter(char safeChar) {
        _safeChar = safeChar;
    }
        
    protected boolean isSafe(byte b) {
        return DEFAULTS[b & 0xFF] != 0;
    }

    @Override
    public String createFilename(StorableKey rawKey) {
        int expLen = Math.max(8, rawKey.length());
        return appendFilename(rawKey, new StringBuilder(expLen)).toString();
    }

    @Override
    public StringBuilder appendFilename(StorableKey rawKey, final StringBuilder sb)
    {
        rawKey.with(new WithBytesCallback<Void>() {
            @Override
            public Void withBytes(byte[] buffer, int offset, int length) {
                final int end = offset+length;
                while (offset < end) {
                    byte b = buffer[offset++];
                    sb.append(isSafe(b) ? (char)b : _safeChar);
                }
                return null;
            }
        });
        return sb;
    }
}