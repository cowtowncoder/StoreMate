package com.fasterxml.storemate.client.call;

import java.io.*;

import com.fasterxml.storemate.shared.ByteContainer;

/**
 * Helper class used for creating one of standard {@link PutContentProvider}
 * implementations.
 */
public class PutContentProviders
{
    public static PutContentProvider forFile(File f, long length) {
        return new FileBacked(f, length);
    }

    public static PutContentProvider forBytes(byte[] bytes) {
        return forBytes(bytes, 0, bytes.length);
    }

    public static PutContentProvider forBytes(byte[] bytes, int offset, int len) {
        return new ByteBacked(ByteContainer.simple(bytes, offset, len));
    }
	
    /*
    ///////////////////////////////////////////////////////////////////////
    // Simple standard implementation for byte[] backed
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Simple {@link PutContentProvider} implementation that is backed by
     * a raw byte array.
     */
    protected static class ByteBacked implements PutContentProvider
    {
        protected final ByteContainer _bytes;

        public ByteBacked(ByteContainer data) {
            _bytes = data;
        }

        @Override
        public long length() {
            return (long) _bytes.byteLength();
        }
        
        @Override
        public ByteContainer contentAsBytes() {
            return _bytes;
        }

        @Override
        public File contentAsFile() {
            return null;
        }
        
        @Override
        public InputStream contentAsStream() {
            return null;
        }
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Simple standard implementations for providers
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Simple {@link PutContentProvider} implementation that is backed by
     * a File.
     */
    protected static class FileBacked implements PutContentProvider
    {
        protected final File _file;
        protected final long _length;

        public FileBacked(File file, long length)
        {
            _file = file;
            _length = length;
        }

        @Override
        public long length() {
            return _length;
        }

        @Override
        public ByteContainer contentAsBytes() {
            return null;
        }

        @Override
        public File contentAsFile() {
            return _file;
        }
        
        @Override
        public InputStream contentAsStream() {
            return null;
        }
    }
}
