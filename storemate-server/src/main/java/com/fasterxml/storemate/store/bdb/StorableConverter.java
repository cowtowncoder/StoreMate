package com.fasterxml.storemate.store.bdb;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.file.FileReference;
import com.fasterxml.storemate.store.util.BytesToStuff;
import com.fasterxml.storemate.store.util.StuffToBytes;


/**
 * Helper class that hides most of complexities on converting between
 * "raw" data stored in BDB, and {@link Storable} value abstraction.
 */
public class StorableConverter
{
    /*
    /**********************************************************************
    /* Offsets
    /**********************************************************************
     */

    public final static int OFFSET_LASTMOD = 0;

    public final static int OFFSET_VERSION = 8; // data format version (for upgrades)
    public final static int OFFSET_STATUS = 9; // available, deleted (tombstone)
    public final static int OFFSET_COMPRESSION = 10; // 
    public final static int OFFSET_EXT_PATH_LENGTH = 11; // 

    public final static int OFFSET_CONTENT_HASH = 12;
    
    /*
    /**********************************************************************
    /* Constant values
    /**********************************************************************
     */
    
    /**
     * Currently we only support the initial version
     */
    public final static int VERSION_1 = 0x11;

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    public StorableConverter() { }
    
    /*
    /**********************************************************************
    /* Public API, reading from DB to Storable
    /**********************************************************************
     */
    
    public Storable decode(final byte[] raw) {
        return decode(raw, 0, raw.length);
    }

    public Storable decode(final byte[] raw, final int offset, final int length)
    {
        // as per Javadocs, offset always 0, size same as arrays:
        BytesToStuff reader = new BytesToStuff(raw, offset, length);

        final long lastmod = reader.nextLong();

        // Ok: ensure version number is valid
        _verifyVersion(reader.nextByte());
        
        final boolean deleted = _decodeStatus(reader.nextByte());
        final Compression compression = _decodeCompression(reader.nextByte());
        final int externalPathLength = reader.nextByte() & 0xFF;

        final int contentHash = reader.nextInt();
        final long originalLength;
        final int compressedHash;
        
        // and now get to variable parts
        if (compression != Compression.NONE) {
            compressedHash = reader.nextInt();
            originalLength = reader.nextVLong();
        } else {
            compressedHash = 0;
            originalLength = -1;
        }
        final int metadataLength = reader.nextVInt();
        final int metadataOffset = reader.offset();

        reader.skip(metadataLength);

        final long storageLength = reader.nextVLong();
        final int payloadOffset = reader.offset();

        // and one more branch: inlined or external storage?
        if (externalPathLength > 0) { // external; should only have ext path in there
            reader.skip(externalPathLength);
        } else { // inline, should have data there
            reader.skip((int) storageLength);
        }
        // and finally, should all add up...
        int left = reader.left();
        if (left > 0) {
            throw new IllegalArgumentException("Had "+left+" bytes left after decoding entry (out of "
                    +raw.length+")");
        }
        return new Storable(raw, offset, length,
                lastmod,
                deleted, compression, externalPathLength,
                contentHash, compressedHash, originalLength,
                metadataOffset, metadataLength,
                payloadOffset, storageLength
                );
    }

    /*
    /**********************************************************************
    /* Public API, converting from storable pieces into DB entry
    /**********************************************************************
     */

    public Storable encodeInlined(StorableKey key, long modtime,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            byte[] inlineData, int inlineOffset, int inlineLength)
    {
        // Guesstimate size we need first (assuming max lengths for varlen fields)
        int len = StuffToBytes.BASE_LENGTH;
        if (stdMetadata.usesCompression()) {
            len += (4 + StuffToBytes.MAX_VLONG_LENGTH);
        }
        // metadata section
        if (customMetadata == null) {
            len += 1; // zero-length marker
        } else {
            len += (StuffToBytes.MAX_VINT_LENGTH + customMetadata.byteLength());
        }
        // and inlined reference
        len += (StuffToBytes.MAX_VINT_LENGTH + inlineLength);
        
        StuffToBytes writer = new StuffToBytes(len);
        
        writer.appendLong(modtime);
        
        return null;
    }

    public Storable encodeOfflined(StorableKey key, long modtime,
            StorableCreationMetadata metadata, ByteContainer customMetadata,
            FileReference externalData)
    {
        return null;
    }
    
    /*
    /**********************************************************************
    /* Helper methods, conversions
    /**********************************************************************
     */

    protected void _verifyVersion(byte b) throws IllegalArgumentException
    {
        int v = (int) b;
        if (v != VERSION_1) {
            throw new IllegalArgumentException("Unsupported version number: 0x"+Integer.toHexString(v)
                    +" (currently only supporting 0x"+Integer.toHexString(VERSION_1));
        }
    }
    
    protected boolean _decodeStatus(byte b) throws IllegalArgumentException
    {
        switch ((int) b) {
        case 0: // available
            return false;
        case 1: // deleted (tombstone)
            return true;
        }
        throw new IllegalArgumentException("Unrecognized status value of "+(b & 0xFF));
    }

    protected Compression _decodeCompression(byte b) throws IllegalArgumentException {
        return Compression.valueOf((int) b, true);
    }
}
