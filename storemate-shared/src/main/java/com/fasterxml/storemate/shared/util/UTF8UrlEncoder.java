package com.fasterxml.storemate.shared.util;

import java.util.Arrays;

/**
 * Convenience class for doing so-called "percent encoding" (and decoding),
 * as defined by RFC-3986, see [http://www.ietf.org/rfc/rfc3986.txt]).
 *<p>
 * May seem like an overkill, but profiling showed that we tend to use
 * non-trivial amount of time in these methods, so assuming there is some
 * value in trying to keep these tight.
 */
public class UTF8UrlEncoder
{
    /**
     * Encoding table used for figuring out ASCII characters that must be escaped
     * (all non-ASCII characers need to be encoded anyway)
     */
    private final static int[] SAFE_ASCII_NO_SLASH = new int[128];
    static {
        for (int i = 'a'; i <= 'z'; ++i) {
            SAFE_ASCII_NO_SLASH[i] = 1;
        }
        for (int i = 'A'; i <= 'Z'; ++i) {
            SAFE_ASCII_NO_SLASH[i] = 1;
        }
        for (int i = '0'; i <= '9'; ++i) {
            SAFE_ASCII_NO_SLASH[i] = 1;
        }
        SAFE_ASCII_NO_SLASH['-'] = 1;
        SAFE_ASCII_NO_SLASH['.'] = 1;
        SAFE_ASCII_NO_SLASH['_'] = 1;
        SAFE_ASCII_NO_SLASH['~'] = 1;
    }

    private final static int[] SAFE_ASCII_WITH_SLASH = new int[SAFE_ASCII_NO_SLASH.length];
    static {
        System.arraycopy(SAFE_ASCII_NO_SLASH, 0, SAFE_ASCII_WITH_SLASH, 0,
                SAFE_ASCII_NO_SLASH.length);
        SAFE_ASCII_WITH_SLASH['/'] = 1;
    }
    
    private final static char[] HEX = "0123456789ABCDEF".toCharArray();

    private final static int[] REVERSE_HEX;
    static {
        final int[] reverse = new int[128];
        Arrays.fill(reverse, -1);
        for (int i = 0; i <= 9; ++i) {
            reverse['0' + i] = i;
        }
        for (int i = 0; i <= 6; ++i) {
            reverse['a' + i] = 10 + i;
            reverse['A' + i] = 10 + i;
        }
        REVERSE_HEX = reverse;
    }
    
    private final boolean _encodeSpaceUsingPlus;

    public UTF8UrlEncoder() {
        this(false);
    }

    public UTF8UrlEncoder(boolean encodeSpaceUsingPlus)
    {
        _encodeSpaceUsingPlus = encodeSpaceUsingPlus;
    }

    /*
    /**********************************************************************
    /* Encoding
    /**********************************************************************
     */

    public String encode(String input, boolean escapeSlash) {
        StringBuilder sb = new StringBuilder(input.length() + 16);
        appendEncoded(sb, input, escapeSlash);
        return sb.toString();
    }

    public StringBuilder appendEncoded(StringBuilder sb, String input,
            boolean escapeSlash)
    {
        final int[] safe = escapeSlash ? SAFE_ASCII_NO_SLASH : SAFE_ASCII_WITH_SLASH;
        for (int i = 0, len = input.length(); i < len; ++i) {
            char c = input.charAt(i);
            if (c <= 127) {
                if (safe[c] != 0) {
                    sb.append(c);
                } else {
                    appendSingleByteEncoded(sb, c);
                }
            } else {
                appendMultiByteEncoded(sb, c);
            }
        }
        return sb;
    }

    private final void appendSingleByteEncoded(StringBuilder sb, int value)
    {
        if (_encodeSpaceUsingPlus && value == 32) {
            sb.append('+');
            return;
        }
        sb.append('%');
        sb.append(HEX[value >> 4]);
        sb.append(HEX[value & 0xF]);
    }

    private final void appendMultiByteEncoded(StringBuilder sb, int value)
    {
        // two or three bytes? (ignoring surrogate pairs for now, which would yield 4 bytes)
        if (value < 0x800) {
            appendSingleByteEncoded(sb, (0xc0 | (value >> 6)));
            appendSingleByteEncoded(sb, (0x80 | (value & 0x3f)));
        } else {
            appendSingleByteEncoded(sb, (0xe0 | (value >> 12)));
            appendSingleByteEncoded(sb, (0x80 | ((value >> 6) & 0x3f)));
            appendSingleByteEncoded(sb, (0x80 | (value & 0x3f)));
        }
    }

    /*
    /**********************************************************************
    /* Decoding
    /**********************************************************************
     */

    public String decode(String input)
    {
        final int len = input.length();
        if (len == 0) {
            return "";
        }
        
        int i = 0;
        // First scan to see if we can avoid any and all work.
        for (; i < len; ++i) {
            char c = input.charAt(i);
            if (c == '+' || c == '%') {
                break;
            }
        }
        if (i == len) {
            return input;
        }
        
        // If not, do the real work
        StringBuilder sb = new StringBuilder(len);
        for (int k = 0; k < i; ++k) {
            sb.append(input.charAt(k));
        }
        
        do {
            char c = input.charAt(i++);
            if (c == '+') {
                sb.append(' ');
            } else if (c == '%') { // offline decoding
                i = _decodeEscaped(input, i, sb);
            } else {
                sb.append(c);
            }
        } while (i < len);
        return sb.toString();
    }

    private final static int _decodeEscaped(String input, int i, StringBuilder sb)
    {
        int first = _decodeSingleEscaped(input, i);
        if (first < 0) {
            sb.append('%');
            return i;
        }
        // Ok: got one, good
        i += 2;
        if (first <= 0x7F) { // ASCII? we're done if so
            sb.append((char) first);
            return i;
        }
        // otherwise, maybe more
        final int len = input.length();
        int second;
        
        if (i >= len || input.charAt(i) != '%'
                || ( second = _decodeSingleEscaped(input, i+1)) < 0) { // or... not
            // this is corrupt or invalid, but let's not freak out
            sb.append((char) first);
            return i;
        }

        i += 3;
        // Two or three bytes to combine?
        if (first < 0xe0) { // two
            first = (first & 0x1F) << 6;
            second = (second & 0x3F);
            sb.append((char) (first | second));
            return i;
        }
        // Or, possibly three... if we have room
        int third;
        if (i >= len || input.charAt(i) != '%'
                || ( third = _decodeSingleEscaped(input, i+1)) < 0) {
            // nope; no such luck. Of bad options, assume first two chars to be added as ASCII...
            sb.append((char) first);
            sb.append((char) second);
            return i;
        }
        i += 3;
        first = (first & 0xF) << 12;
        second = (second & 0x3F) << 6;
        third = (second & 0x3F) << 6;
        sb.append((char) (first | second | third));
        return i;
    }
        
    private final static int _decodeSingleEscaped(String input, int i)
    {
        final int end = input.length();

        // first: must get 2 more chars, minimum
        if ((i + 2) < end) {
            // and they must be hex chars
            char c1 = input.charAt(i);
            char c2 = input.charAt(i+1);
            
            if (c1 < 127 && c2 < 127) {
                int h1 = REVERSE_HEX[c1];
                int h2 = REVERSE_HEX[c2];
                if (h1 >= 0 && h2 >= 0) {
                    return (h1 << 4) + h2;
                }
            }
        }
        return -1;
    }
}
