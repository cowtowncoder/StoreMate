package com.fasterxml.storemate.shared;

/**
 * Simple {@link WithBytesCallback} implementation to use when you
 * just want a {@link ByteContainer} as a result.
 */
public class WithBytesAsContainer implements WithBytesCallback<ByteContainer>
{
    public final static WithBytesAsContainer instance = new WithBytesAsContainer();

    @Override
    public ByteContainer withBytes(byte[] buffer, int offset, int length) {
        if (offset == 0) {
            return ByteContainer.emptyContainer();
        }
        return ByteContainer.simple(buffer, offset, length);
    }
}
