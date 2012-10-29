package com.fasterxml.storemate.shared.util;

/**
 * Convenience class for doing so-called "percent encoding", as defined
 * by RFC-3986, see [http://www.ietf.org/rfc/rfc3986.txt]).
 */
public class UTF8UrlEncoder
{
    /**
     * Encoding table used for figuring out ASCII characters that must be escaped
     * (all non-ASCII characers need to be encoded anyway)
     */
    private final static int[] SAFE_ASCII = new int[128];

    static {
        for (int i = 'a'; i <= 'z'; ++i) {
            SAFE_ASCII[i] = 1;
        }
        for (int i = 'A'; i <= 'Z'; ++i) {
            SAFE_ASCII[i] = 1;
        }
        for (int i = '0'; i <= '9'; ++i) {
            SAFE_ASCII[i] = 1;
        }
        SAFE_ASCII['-'] = 1;
        SAFE_ASCII['.'] = 1;
        SAFE_ASCII['_'] = 1;
        SAFE_ASCII['~'] = 1;
    }

    private final static char[] HEX = "0123456789ABCDEF".toCharArray();

    private final boolean _encodeSpaceUsingPlus;

    public UTF8UrlEncoder() {
        this(false);
    }

    public UTF8UrlEncoder(boolean encodeSpaceUsingPlus)
    {
        _encodeSpaceUsingPlus = encodeSpaceUsingPlus;
    }

    public String encode(String input) {
        StringBuilder sb = new StringBuilder(input.length() + 16);
        appendEncoded(sb, input);
        return sb.toString();
    }

    public StringBuilder appendEncoded(StringBuilder sb, String input)
    {
        final int[] safe = SAFE_ASCII;
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
}
