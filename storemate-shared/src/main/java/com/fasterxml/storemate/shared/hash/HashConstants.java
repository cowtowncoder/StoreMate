package com.fasterxml.storemate.shared.hash;

public abstract class HashConstants
{
    /**
     * Placeholder used for indicating that no checksum is available.
     */
    public final static int NO_CHECKSUM = 0;

    /**
     * Since we can not use value {@link NO_CHECKSUM} (which would be calculated
     * for zero-length byte array, for example), this is the value used to
     * mask it
     */
    public final static int CHECKSUM_FOR_ZERO = 1;

}
