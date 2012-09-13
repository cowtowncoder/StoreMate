package com.fasterxml.storemate.shared.compress;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.WithBytesCallback;
import com.ning.compress.lzf.LZFChunk;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;

public class Compressors
{
    /*
    /**********************************************************************
    /* Verification
    /**********************************************************************
     */

    public static boolean isCompressed(byte[] data, int offset, int len) {
        return findCompression(data, offset, len) != null;
    }

    public static Compression findCompression(byte[] data, int offset, int len)
    {
        if (len < 3) {
            return null;
        }
        byte b = data[offset];
        if (b == LZFChunk.BYTE_Z) { // LZF: // starts with 'ZV' == 0x5A, 0x56
            if (data[offset+1] == LZFChunk.BYTE_V) {
                byte third = data[offset+2];
                if (third == LZFChunk.BLOCK_TYPE_COMPRESSED || third == LZFChunk.BLOCK_TYPE_NON_COMPRESSED) {
                    return Compression.LZF;
                }
            }
        } else if (b == 0x1F) { // GZIP: // starts with 0x1F, 0x8B (0x8B1F, little-endian)
            if ((data[offset+1] & 0xFF) == 0x8B) {
                return Compression.GZIP;
            }
        }
        return null;
    }
    
    /*
    /**********************************************************************
    /* Compress
    /**********************************************************************
     */

    public static byte[] gzipCompress(byte[] data) throws IOException {
        return gzipCompress(data, 0, data.length);
    }

    public static byte[] gzipCompress(byte[] data, int offset, int len) throws IOException
    {
        // assume 50% compression rate
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(len);
        GZIPOutputStream out = new GZIPOutputStream(bytes);
        out.write(data, offset, len);
        out.close();
        return bytes.toByteArray();
    }

    public static byte[] lzfCompress(byte[] data) throws IOException {
        return lzfCompress(data, 0, data.length);
    }

    public static byte[] lzfCompress(byte[] data, int offset, int len) throws IOException {
        return LZFEncoder.encode(data, offset, len);
    }

    public static OutputStream compressingStream(OutputStream out, Compression comp) throws IOException
    {
        if (comp != null) {
            switch (comp) {
            case NONE:
                break;
            case LZF:
                return new LZFOutputStream(out);
            case GZIP:
                return new GZIPOutputStream(out);
            default: // sanity check
                throw new IllegalArgumentException("Unrecognized compression type: "+comp);
            }
        }
        return out;
    }
    
    /*
    /**********************************************************************
    /* Uncompress
    /**********************************************************************
     */
    
    public static InputStream uncompressingStream(InputStream in, Compression comp)
        throws IOException
    {
        if (comp != null) {
            switch (comp) {
            case NONE:
                break;
            case LZF:
                return new LZFInputStream(in);
            case GZIP:
                return new GZIPInputStream(in);
            default: // sanity check
                throw new IllegalArgumentException("Unrecognized compression type: "+comp);
            }
        }
        return in;
    }

    public static ByteContainer uncompress(ByteContainer data, Compression comp, int expSize)
            throws IOException
    {
        if (comp != null) {
            switch (comp) {
            case NONE:
                break;
            case LZF:
                return lzfUncompress(data);
            case GZIP:
                return gzipUncompress(data, expSize);
            default: // sanity check
                throw new IllegalArgumentException("Unrecognized compression type: "+comp);
            }
        }
        return data;
    }

    public static ByteContainer gzipUncompress(ByteContainer compData, int expSize)
        throws IOException
    {
    	return ByteContainer.simple(gzipUncompress(compData.asBytes(), expSize));
    }
    
    public static byte[] gzipUncompress(byte[] compData, int expSize)
            throws IOException
    {
        if (expSize <= 0) {
            return gzipUncompress(compData);
        }
        byte[] buffer = new byte[expSize];
        GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(compData));
        int offset = 0;
        int left = buffer.length;
        int count;

        while (left > 0 && (count = in.read(buffer, offset, left)) > 0) {
            offset += count;
            left -= count;
        }
        // should have gotten exactly expected amount
        if (offset < expSize) {
            throw new IOException("Corrupt GZIP/Deflate data: expected "+expSize+" bytes, got "+offset);
        }
        // and no more
        if (in.read() != -1) {
            throw new IOException("Corrupt GZIP/Deflate data: expected "+expSize+" bytes, got at least one more");
        }
        in.close();
        return buffer;
    }

    public static byte[] gzipUncompress(byte[] compData)
        throws IOException
    {
        GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(compData));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(16 + (compData.length << 1));
        byte[] buffer = new byte[500];
        int count;

        while ((count = in.read(buffer)) > 0) {
            bytes.write(buffer, 0, count);
        }
        in.close();
        bytes.close();
        return bytes.toByteArray();
    }
    
    public static byte[] lzfUncompress(byte[] data) throws IOException
    {
        return LZFDecoder.decode(data);
    }

    public static ByteContainer lzfUncompress(ByteContainer data) throws IOException
    {
    	return data.withBytes(new WithBytesCallback<ByteContainer>() {
			@Override
			public ByteContainer withBytes(byte[] buffer, int offset, int length)
				throws IllegalArgumentException
			{
				try {
					return ByteContainer.simple(LZFDecoder.decode(buffer, offset, length));
				} catch (IOException e) {
					throw new IllegalArgumentException("Bad LZF data to uncompress ("+length+" bytes): "+e.getMessage(), e);
				}
			}
    	});
    }
}
