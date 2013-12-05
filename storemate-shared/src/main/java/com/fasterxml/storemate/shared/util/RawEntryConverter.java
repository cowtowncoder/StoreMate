package com.fasterxml.storemate.shared.util;

import java.io.IOException;

/**
 * Interface used for converters needed to convert between raw entries
 * and actual Object representations.
 */
public abstract class RawEntryConverter<T>
{
    public T fromRaw(byte[] raw) throws IOException {
        return fromRaw(raw, 0, raw.length);
    }
    
    public abstract T fromRaw(byte[] raw, int offset, int length) throws IOException;

    public abstract byte[] toRaw(T value) throws IOException;
}
