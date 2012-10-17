package com.fasterxml.storemate.service;

import java.io.ByteArrayOutputStream;

/**
 * Value class used for encapsulating details of client ids.
 * Client ids are internally stored as 32-bit signed ints; but
 * externally can use one of two representations:
 *<ul>
 * <li>Numeric value between 0 and 999999 (where 0 is reserved to mean "unknown").
 *    We may increase this range in future, approximately to hex value of
 *    <code>0x40FFFFFF</code> if necessary (this is upper limit since valid
 *    4-character values may start with 'A') -- that is, approximately
 *    one billion (nine nines) -- if we have to.
 *   </li>
 * <li>Four-character id that starts with a capital letter, and otherwise consists
 *    of capital letters, numbers and underscores.
 *   </li>
 * </ul>
 * No other representations are valid; and trying to use them results in exception.
 * These external representations are non-overlapping, so we can uniquely determine
 * mapping between internal number and expected representations; as well as
 * verify validity of either pure numeric value, or alleged String representation.
 *<p>
 * Intended usage is such that production Client Ids are expected to use mnemonic
 * (4-character) representation; and unit tests to use numeric ones. However
 */
public final class ClientId
{
    // Fundamental limit we enforce is that we allow up to 9 digits (not including
    // ignorable leading zeroes)
    private final static int MAX_DIGITS = 9;

    // protected to give unit test access
    protected final static int MAX_NUMERIC = 999999;

    private final static int MAX_NON_MNEMONIC = 0x40FFFFFF;
    
    /**
     * We use this marker value to denote internal value of 0, which is typically
     * used as sort of "null Object".
     */
    public final static ClientId NA = new ClientId(0, "0");
    
    protected final int _value;

    protected volatile String _asString;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected ClientId(int internalValue, String stringRepr) {
        _value = internalValue;
        _asString = stringRepr;
    }
    
    public static ClientId valueOf(int numeric)
    {
        if (numeric <= 0) {
            if (numeric == 0) {
                return NA;
            }
            throw new IllegalArgumentException("Invalid Client Id ("+numeric+"): can not be negative number");
        }
        if (numeric <= MAX_NUMERIC) {
            return new ClientId(numeric, null);
        }
        // Intermediate range...
        if (numeric <= MAX_NON_MNEMONIC) {
            _throwTooBig(String.valueOf(numeric));
        }
        // And then "numeric version of mnenomic" case
        int ch = (numeric >>> 24);
        if (ch < 'A' || ch > 'Z') {
            throw new IllegalArgumentException("Invalid Client Id ("+numeric+"): first character code (0x"
                    +Integer.toHexString(ch)+" is not an upper-case letter");
        }
        ch = (ch << 8) | _validateChar(numeric >> 16 , 2, numeric);
        ch = (ch << 8) | _validateChar(numeric >> 8, 3, numeric);
        ch = (ch << 8) | _validateChar(numeric, 4, numeric);
        return new ClientId(ch, null);
    }

    public static ClientId valueOf(String mnemonic)
    {
        int len = (mnemonic == null) ? 0 : mnemonic.length();
        if (len == 0) {
            return NA;
        }
        // First things first: numeric or mnenomic? First character determines
        int ch = mnemonic.charAt(0);
        if (ch <= 'Z' && ch >= 'A') {
            if (len != 4) {
                throw new IllegalArgumentException("Invalid textual Client Id '"+mnemonic+"': has to be 4 characters long");
            }
            return _menomicValueOf(mnemonic, ch);
        }
        if (ch <= '9' && ch >= '0') {
            return _numericValueOf(mnemonic, ch, len);
        }
        throw new IllegalArgumentException("Invalid Client Id '"+mnemonic+"': does not start with upper-case letter or digit");
    }

    public static ClientId from(byte[] buffer, int offset)
    {
        int rawId = ((buffer[offset++]) << 24)
                | ((buffer[offset++] & 0xFF) << 16)
                | ((buffer[offset++] & 0xFF) << 8)
                | (buffer[offset++] & 0xFF)
                ;
        return valueOf(rawId);
    }
    
    protected static ClientId _menomicValueOf(String mnemonic, int ch)
    {
        // and obey the other constraints: start with upper case ASCII letter (A-Z)
        // and then have 3 upper-case ASCII letters, numbers and/or underscores
        ch = (ch << 8) | _validateChar(mnemonic, 1);
        ch = (ch << 8) | _validateChar(mnemonic, 2);
        ch = (ch << 8) | _validateChar(mnemonic, 3);
        return new ClientId(ch, mnemonic);
    }

    protected static ClientId _numericValueOf(String mnemonic, int ch, final int len)
    {
        ch -= '0';
        // first: trim leading zeroes, if any; also handles "all zeroes" case
        int i = 1;
        if (ch == 0) {
            while (true) {
                if (i == len) { // all zeroes
                    return NA;
                }
                if (mnemonic.charAt(i) != '0') {
                    break;
                }
                ++i;
            }
        }
        
        // Then ensure it's all numbers; enforce max length as well
        int nonZeroDigits = 0;
        for (; i < len; ++i) {
            int c = mnemonic.charAt(i);
            if (c > '9' || c < '0') {
                throw new IllegalArgumentException("Invalid numeric Client Id '"+mnemonic+"': contains non-digit characters");
            }
            if (++nonZeroDigits > MAX_DIGITS) {
                _throwTooBig(mnemonic);
            }
            ch = (ch * 10) + (c - '0');
        }
        // may still have too big magnitude
        if (ch > MAX_NUMERIC) {
            _throwTooBig(mnemonic);
        }
        // let's not pass String as it may be non-canonical (leading zeroes)
        return new ClientId(ch, null);
    }

    private static void _throwTooBig(String idStr) {
        throw new IllegalArgumentException("Invalid Client Id ("+idStr+"): numeric ids can not exceed "+MAX_NUMERIC);
    }
    
    private static int _validateChar(String mnemonic, int index)
    {
        char c = mnemonic.charAt(index);
        if (_isValidChar(c)) {
            return c;
        }
        throw new IllegalArgumentException("Invalid Client Id '"+mnemonic+"': character #"+index
                +" invalid; has to be an upper-case letter, number or underscore");
    }

    private static int _validateChar(int ch, int index, int full)
    {
        ch = ch & 0xFF;
        if (_isValidChar(ch)) {
            return ch;
        }
        throw new IllegalArgumentException("Invalid byte #"+index+"/4 of alleged Client Id 0x"+Integer.toHexString(full)
                +": has to be an upper-case letter, number or underscore");
    }
    
    private final static boolean _isValidChar(int ch)
    {
        return ((ch <= 'Z' && ch >= 'A')
                || (ch <= '9' && ch >= '0')
                || (ch == '_'));
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Accessors
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * 
     * @return True if value is "mnemonic", that is, expressed as 4-character
     *    String; false if not (and is expressed as number)
     */
    public boolean isMnemonic()
    {
        return (_value > MAX_NON_MNEMONIC);
    }
    
    /**
     * Accessor for getting the internal 32-bit int representation, used
     * for efficient storage.
     */
    public int asInt() {
        return _value;
    }

    public StringBuilder append(StringBuilder sb)
    {
        // TODO: optimize if necessary (can avoid String construction)
        sb.append(toString());
        return sb;
    }

    public ByteArrayOutputStream append(ByteArrayOutputStream bytes)
    {
        bytes.write(_value >> 24);
        bytes.write(_value >> 16);
        bytes.write(_value >> 8);
        bytes.write(_value);
        return bytes;
    }

    public int append(byte[] buffer, int offset)
    {
        buffer[offset++] = (byte)(_value >> 24);
        buffer[offset++] = (byte)(_value >> 16);
        buffer[offset++] = (byte)(_value >> 8);
        buffer[offset++] = (byte) _value;
        return offset;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Overridden standard methods
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public String toString() {
        String str = _asString;
        if (str == null) {
            str = _toString(_value);
            _asString = str;
        }
        return str;
    }

    public static String toString(int raw) {
        return _toString(raw);
    }
    
    @Override public int hashCode() { return _value; }
    
    @Override
    public boolean equals(Object other)
    {
        if (other == this) return true;
        if (other == null) return false;
        if (other.getClass() != getClass()) return false;
        return ((ClientId) other)._value == _value;
    }

    private final static String _toString(int value)
    {
        if (value <= MAX_NON_MNEMONIC) {
            return String.valueOf(value);
        }
        StringBuilder sb = new StringBuilder(4);
        sb.append((char) (value >>> 24));
        sb.append((char) ((value >> 16) & 0xFF));
        sb.append((char) ((value >> 8) & 0xFF));
        sb.append((char) (value & 0xFF));
        return sb.toString();
    }

}
