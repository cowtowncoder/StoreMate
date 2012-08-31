package com.fasterxml.storemate.store.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;

public class IOUtil
{
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
        out.write(data, offset, length);
        out.close();
    }
    
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

}
