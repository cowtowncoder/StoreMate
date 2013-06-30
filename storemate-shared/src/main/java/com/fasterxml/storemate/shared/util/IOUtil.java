package com.fasterxml.storemate.shared.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;

public class IOUtil
{
    private final static int MAX_EXCERPT_BYTES = 500;

    private final static byte[] NO_BYTES = new byte[0];

    /*
    /**********************************************************************
    /* Helpers methods to deal with HTTP
    /**********************************************************************
     */
	
    public static boolean isHTTPSuccess(int statusCode)
    {
        return (statusCode >= 200) && (statusCode <= 299);
    }
	
    /*
    /**********************************************************************
    /* Read/write helpers for I/O
    /**********************************************************************
     */

    public static int readFully(InputStream in, byte[] buffer) throws IOException
    {
        int offset = 0;

        while (true) {
            int toRead = buffer.length - offset;
            if (toRead < 1) {
                break;
            }
            int count = in.read(buffer, offset, toRead);
            if (count <= 0) {
                if (count == 0) {
                    throw new IOException("Broken InputStream: returned 0 bytes when requested "+toRead+"!");
                }
                break;
            }
            offset += count;
        }
        return offset;
    }
    
    public static void writeFile(File file, byte[] data, int offset, int length)
            throws IOException
    {
        FileOutputStream out = new FileOutputStream(file);
        try {
            out.write(data, offset, length);
        } finally {
            out.close();
        }
    }

    public static void writeFile(File file, ByteContainer data)
        throws IOException
    {
        FileOutputStream out = new FileOutputStream(file);
        try {
            data.writeBytes(out);
        } finally {
            out.close();
        }
    }
    
    /*
    /**********************************************************************
    /* Helpers for compression handling
    /**********************************************************************
     */
    
    public static String verifyCompression(Compression alleged, byte[] readBuffer, int len)
    {
        if (alleged != null && alleged != Compression.NONE) {
            Compression actual = Compressors.findCompression(readBuffer, 0, len);
            if (actual != alleged) {
                if (len < 3) { // LZF minimum is 3 bytes for empty file...
                    return "Invalid compression '"+alleged+"': payload length only "
                                +len+" bytes; can not be compressed data";
                }
                return "Invalid compression '"+alleged
                        +"': does not start with expected bytes; first three bytes are:"
                        +" "+Integer.toHexString(readBuffer[0] & 0xFF)
                        +" "+Integer.toHexString(readBuffer[1] & 0xFF)
                        +" "+Integer.toHexString(readBuffer[2] & 0xFF)
                        ;
            }
        }
        return null;
    }

    public static String verifyCompression(Compression alleged, ByteContainer input)
    {
        if (alleged != null && alleged != Compression.NONE) {
            Compression actual = Compressors.findCompression(input);
            if (actual != alleged) {
                if (input.byteLength() < 3L) { // LZF minimum is 3 bytes for empty file...
                    return "Invalid compression '"+alleged+"': payload length only "
                    +input.byteLength()+" bytes; can not be compressed data";
                }
                return "Invalid compression '"+alleged
                        +"': does not start with expected bytes; first three bytes are:"
                        +" "+Integer.toHexString(input.get(0) & 0xFF)
                        +" "+Integer.toHexString(input.get(1) & 0xFF)
                        +" "+Integer.toHexString(input.get(2) & 0xFF)
                        ;
            }
        }
        return null;
    }
    
    /*
    /**********************************************************************
    /* Helper methods for error handling
    /**********************************************************************
     */

    public static String getExcerpt(byte[] stuff)
    {
        final int end = Math.min(MAX_EXCERPT_BYTES, stuff.length);
        try {   
            return new String(stuff, 0, end, "ISO-8859-1");
        } catch (Exception e) {
            return "N/A";
        }
    }

    public static String getExcerpt(InputStream in)
    {
        try {
            byte[] buffer = new byte[MAX_EXCERPT_BYTES];
            int offset = 0;
            int count;
            while ((count = in.read(buffer, offset, buffer.length-offset)) > 0) {
                offset += count;
            }
            if (offset == 0) {
                return "";
            }
            return new String(buffer, 0, offset, "ISO-8859-1");
        } catch (Exception e) {
            return "N/A";
        }
    }

    /*
    /**********************************************************************
    /* Handling of ASCII conversions
    /**********************************************************************
     */
    
    /**
     * Helper method for constructing a String from bytes assumed to be
     * 7-bit ASCII characters, given a {@link ByteContainer}.
     */
    public static String getAsciiString(ByteContainer bytes) {
        return bytes.withBytes(AsciiViaCallback.instance);
    }
    
    public static String getAsciiString(byte[] bytes) {
        return getAsciiString(bytes, 0, bytes.length);
    }
    
    public static String getAsciiString(byte[] bytes, int offset, int length)
    {
        if (bytes == null) return null;
        if (length == 0) return "";
        StringBuilder sb = new StringBuilder(length);
        for (int i = offset, end = offset+length; i < end; ++i) {
            // due to sign extension, 0x80-0xFF will become "funny"; caller is only to feed ASCII
            sb.append((char) bytes[i]);
        }
        return sb.toString();
    }
    
    public static byte[] getAsciiBytes(String str)
    {
        if (str == null) return null;
        final int len = str.length();
        if (len == 0) return NO_BYTES;
        final byte[] result = new byte[len];
        for (int i = 0; i < len; ++i) {
            result[i] = (byte) str.charAt(i);
        }
        return result;
    }

    /*
    /**********************************************************************
    /* Helper classes
    /**********************************************************************
     */
    
    private final static class AsciiViaCallback implements WithBytesCallback<String>
    {
        public final static AsciiViaCallback instance = new AsciiViaCallback();
        
        @Override
        public String withBytes(byte[] buffer, int offset, int length) {
            return getAsciiString(buffer, offset, length);
        }
    }
}
