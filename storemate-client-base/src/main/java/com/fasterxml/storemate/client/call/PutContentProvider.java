package com.fasterxml.storemate.client.call;

import java.io.File;

import com.fasterxml.storemate.shared.ByteContainer;

/**
 * Interface that defines how calling application needs to expose data to upload,
 * so that client can upload it to multiple stores (or for possible
 * retries) as necessary.
 *<p>
 * Methods are typically called in order of:
 *<ol> 
 * <li>{@link #contentAsBytes}</li>
 * <li>{@link #contentAsFile}</li>
 * <li>{@link #contentAsStream}</li>
 *</ol> 
 * until non-null response is received; not that most implementations return null
 * from one or more of methods, to indicate that they are not optimal accessors.
 */
public interface PutContentProvider
{
    /**
     * @return Length of content, if known; -1 if not known
     */
    public long length();
    
    public ByteContainer contentAsBytes();

    public File contentAsFile() throws java.io.IOException;
    
    public java.io.InputStream contentAsStream() throws java.io.IOException;
}
