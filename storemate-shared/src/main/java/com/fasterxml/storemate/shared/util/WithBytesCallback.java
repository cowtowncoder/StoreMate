package com.fasterxml.storemate.shared.util;

public interface WithBytesCallback<T>
{
    public T withBytes(byte[] buffer, int offset, int length);
}
