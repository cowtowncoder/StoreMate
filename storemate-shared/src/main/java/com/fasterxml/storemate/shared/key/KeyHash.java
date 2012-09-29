package com.fasterxml.storemate.shared.key;

/**
 * Simple value class that encapsulates a 32-bit int hash value
 * in context of a specific {@link KeySpace}. Used to hide the
 * details of modulo calculations and reduce likelihood of
 * mismatch between full hash value and hash modulo; latter
 * being used for all calculations within a {@link KeySpace}.
 */
public class KeyHash
{
    private final int _fullHash;

    private final int _moduloHash;
	
    public KeyHash(int fullHash, int modulo)
    {
        _fullHash = fullHash;
        _moduloHash = calcModulo(fullHash, modulo);
    }

    public static int calcModulo(int fullHash, int modulo)
    {
        int h = (fullHash < 0) ? (fullHash & 0x7FFFFFFF) : fullHash;
        return h % modulo;
    }
    
    public int getModuloHash() { return _moduloHash; }
    public int getFullHash() { return _fullHash; }

    @Override
    public String toString()
    {
        return "[KeyHash, full=0x"+Integer.toHexString(_fullHash)+"; modulo=0x"+Integer.toHexString(_moduloHash)+"]";
    }
}
