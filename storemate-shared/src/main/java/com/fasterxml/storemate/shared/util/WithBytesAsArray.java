package com.fasterxml.storemate.shared.util;

import java.util.Arrays;

/**
 * Simple {@link WithBytesCallback} implementation to use when all you
 * really want is a byte array copy.
 */
public class WithBytesAsArray implements WithBytesCallback<byte[]>
{
    public final static WithBytesAsArray instance = new WithBytesAsArray();

    @Override
    public byte[] withBytes(byte[] buffer, int offset, int length) {
        if (offset == 0) {
            return Arrays.copyOf(buffer, length);
        }
        return Arrays.copyOfRange(buffer, offset, offset+length);
    }
}
