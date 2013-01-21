package com.fasterxml.storemate.client.call;

import java.io.*;

/**
 * Base class for objects that are used for converting entities, by
 * decoding and/or parsing entities from stream or chunked input.
 */
public interface ContentConverter<T>
{
    public T convert(InputStream in) throws IOException;

    public T convert(byte[] buffer, int offset, int length) throws IOException;
}
