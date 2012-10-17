package com.fasterxml.storemate.service;

import java.io.*;

/**
 * Interface wrapped around content that is dynamically read and written
 * as part of HTTP response processing.
 */
public interface StreamingResponseContent
{
    public void writeContent(OutputStream out) throws IOException;

    /**
     * Method that may be called to check length of the content to stream,
     * if known. If length is not known, -1 will be returned; otherwise
     * non-negative length.
     */
    public long getLength();
}
