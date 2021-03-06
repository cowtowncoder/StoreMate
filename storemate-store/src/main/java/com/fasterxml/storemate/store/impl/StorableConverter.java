package com.fasterxml.storemate.store.impl;

import java.util.Arrays;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.util.IOUtil;
import com.fasterxml.storemate.shared.util.WithBytesAsArray;
import com.fasterxml.storemate.shared.util.WithBytesCallback;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableCreationMetadata;
import com.fasterxml.storemate.store.file.FileReference;
import com.fasterxml.storemate.store.util.BytesToStuff;
import com.fasterxml.storemate.store.util.StuffToBytes;

/**
 * Helper class that hides most of complexities on converting between
 * "raw" data stored in backend data store, and generic {@link Storable}
 * value abstraction.
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
    public final static byte VERSION_1 = 0x11;

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
    
    public Storable decode(StorableKey key, final byte[] raw) {
        return decode(key, raw, 0, raw.length);
    }

    public Storable decode(StorableKey key,
    		final byte[] raw, final int offset, final int length)
    {
        // as per Javadocs, offset always 0, size same as arrays:
        BytesToStuff reader = new BytesToStuff(raw, offset, length);
        
        /*
for (int i = 0, end = Math.min(length, 24); i < end; ++i) {
    System.out.println("#"+i+" -> 0x"+Integer.toHexString(raw[offset+i] & 0xFF));
}
*/
        final long lastmod = reader.nextLong();

        // Ok: ensure version number is valid
        _verifyVersion(reader.nextByte());
        
        int statusFlags = reader.nextByte();
        final Compression compression = _decodeCompression(reader.nextByte());
        final int externalPathLength = reader.nextByte() & 0xFF;
        
        final int contentHash = reader.nextInt();
        final long originalLength;
        final int compressedHash;
        
        // and now get to variable parts
        if (compression != Compression.NONE) {
            compressedHash = reader.nextInt();
            long l = reader.nextVLong();
            // one work-around: can't use negative values so '0' means N/A
            if (l == 0L) {
                l = -1L;
            }
            originalLength = l;
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
        return new Storable(key, ByteContainer.simple(raw, offset, length),
                lastmod, statusFlags, compression, externalPathLength,
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
            ByteContainer inlineData)
    {
        StuffToBytes estimator = StuffToBytes.estimator();
        _encodeInlined(key, estimator, false,
                modtime, stdMetadata, customMetadata, inlineData);
        StuffToBytes writer = StuffToBytes.writer(estimator.offset());
        return _encodeInlined(key, writer, true,
                modtime, stdMetadata, customMetadata, inlineData);
    }
    
    private Storable _encodeInlined(StorableKey key, StuffToBytes writer, boolean createStorable,
            long modtime,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            ByteContainer inlineData)
    {
        writer.appendLong(modtime)
            .appendByte(VERSION_1) // version
            .appendByte(stdMetadata.statusAsByte()) // status
            .appendByte(stdMetadata.compressionAsByte()) // compression
            .appendByte((byte) 0) // external path length (none for inlined)
            .appendInt(stdMetadata.contentHash)
            ;

        if (stdMetadata.usesCompression()) {
            long uncompLen = stdMetadata.uncompressedSize;
            if (uncompLen == -1L) { // VInts/VLongs not used for negative here, mask
                uncompLen = 0;
            }
            writer.appendInt(stdMetadata.compressedContentHash) // comp hash
                .appendVLong(uncompLen); // orig length
        }
        
        final int metadataOffset = writer.offset();
        final int metadataLength;
        
        // metadata section
        if (customMetadata == null) {
            writer.appendVLong(0L); // just length marker
            metadataLength = 0;
        } else {
            writer.appendLengthAndBytes(customMetadata);
            metadataLength = customMetadata.byteLength();
        }

        final int payloadOffset;
        if (inlineData == null) {
            writer.appendVInt(0);
            payloadOffset = writer.offset();
        } else {
            writer.appendVInt(inlineData.byteLength());
            payloadOffset = writer.offset();
            writer.appendBytes(inlineData);
        }
        if (!createStorable) {
            return null;
        }
        return new Storable(key, writer.bufferedBytes(), modtime,
                stdMetadata.statusAsByte(), stdMetadata.compression, 0,
                stdMetadata.contentHash, stdMetadata.compressedContentHash, stdMetadata.uncompressedSize,
                metadataOffset, metadataLength,
                payloadOffset, stdMetadata.storageSize);
    }

    public Storable encodeOfflined(StorableKey key,
            long modtime,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            FileReference externalData)
    {
        StuffToBytes estimator = StuffToBytes.estimator();
        _encodeOfflined(key, estimator, false,
                modtime, stdMetadata, customMetadata, externalData);
        StuffToBytes writer = StuffToBytes.writer(estimator.offset());
        return _encodeOfflined(key, writer, true,
                modtime, stdMetadata, customMetadata, externalData);
    }

    private Storable _encodeOfflined(StorableKey key, StuffToBytes writer, boolean createStorable,
            long modtime,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            FileReference externalData)
    {
        byte[] rawRef = IOUtil.getAsciiBytes(externalData.getReference());
        if (rawRef.length > 255) { // sanity check
            throw new IllegalStateException("Length of external reference ("+rawRef.length+") exceeds 255");
        }
        writer.appendLong(modtime)
            .appendByte(VERSION_1) // version
            .appendByte(stdMetadata.statusAsByte()) // status
            .appendByte(stdMetadata.compressionAsByte()) // compression
            .appendByte((byte) rawRef.length) // external path length (none for inlined)
            .appendInt(stdMetadata.contentHash)
        ;

        if (stdMetadata.usesCompression()) {
            writer.appendInt(stdMetadata.compressedContentHash) // comp hash
                .appendVLong(stdMetadata.uncompressedSize); // orig length
        }
        
        final int metadataOffset = writer.offset();
        final int metadataLength;
        
        // metadata section
        if (customMetadata == null) {
            writer.appendVLong(0L); // just length marker
            metadataLength = 0;
        } else {
            writer.appendLengthAndBytes(customMetadata);
            metadataLength = customMetadata.byteLength();
        }
        writer.appendVLong(stdMetadata.storageSize);
        // NOTE: although storageSize is technically part of payload section, 'payloadOffset'
        // is to point to actual payload data
        final int payloadOffset = writer.offset();
        writer.appendBytes(rawRef);

        if (!createStorable) {
            return null;
        }
        return new Storable(key, writer.bufferedBytes(), modtime,
                stdMetadata.statusAsByte(), stdMetadata.compression, rawRef.length,
                stdMetadata.contentHash, stdMetadata.compressedContentHash, stdMetadata.uncompressedSize,
                metadataOffset, metadataLength,
                payloadOffset, stdMetadata.storageSize);
    }

    /*
    /**********************************************************************
    /* Public API, modifying instances
    /**********************************************************************
     */

    public Storable softDeletedCopy(StorableKey key, Storable orig, final long deletionTime,
            boolean deleteInlined, boolean deleteExternal)
    {
        final boolean removeExternal = deleteExternal && orig.hasExternalData();
        final boolean removeInlined = deleteInlined && orig.hasInlineData();
        
        /* First things first: if we have to retain external or inlined,
         * it's just a single byte change.
         */
        if (!(removeInlined || removeExternal)) {
            byte[] raw = orig.withRaw(WithBytesAsArray.instance);
            raw[OFFSET_STATUS] |= StorableFlags.F_STATUS_SOFT_DELETED;
            _ovewriteTimestamp(raw, 0, deletionTime);
            return orig.softDeletedCopy(ByteContainer.simple(raw), false, deletionTime);
        }
        /* otherwise we can still make use of first part of data, up to and
         * including optional metadata, and no minor in-place mod on copy
         */
        byte[] base = orig.withRawWithoutPayload(new WithBytesCallback<byte[]>() {
            @Override
            public byte[] withBytes(byte[] buffer, int offset, int length) {
                // minor kink: we need room for one more null byte:
                byte[] result = Arrays.copyOfRange(buffer, offset, length+1);
                result[OFFSET_STATUS] |= StorableFlags.F_STATUS_SOFT_DELETED;
                // Length is a VLong, so:
                result[length] = StuffToBytes.ZERO_LENGTH_AS_BYTE;
                return result;
            }
        });
        // also, clear up external path length
        if (removeExternal) {
            // note: single byte, not variable length, hence plain zero
            base[OFFSET_EXT_PATH_LENGTH] = 0;
        }
        _ovewriteTimestamp(base, 0, deletionTime);
        Storable mod = orig.softDeletedCopy(ByteContainer.simple(base), true, deletionTime);
        return mod;
    }
    
    /*
    /**********************************************************************
    /* Internal helper methods
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
    
    protected boolean _decodeStatusDeleted(byte b) throws IllegalArgumentException {
        return ((b & StorableFlags.F_STATUS_SOFT_DELETED) != 0);
    }

    protected boolean _decodeStatusReplicated(byte b) throws IllegalArgumentException {
        return ((b & StorableFlags.F_STATUS_REPLICATED) != 0);
    }
    
    protected void _ovewriteTimestamp(byte[] buffer, int offset, long time)
    {
        offset += OFFSET_LASTMOD;
        _putIntBE(buffer, offset, (int) (time >> 32));
        _putIntBE(buffer, offset+4, (int) time);
    }

    private void _putIntBE(byte[] buffer, int offset, int value)
    {
        buffer[offset++] = (byte) (value >> 24);
        buffer[offset++] = (byte) (value >> 16);
        buffer[offset++] = (byte) (value >> 8);
        buffer[offset++] = (byte) value;
    }
    
    protected Compression _decodeCompression(byte b) throws IllegalArgumentException {
        return Compression.forIndex((int) b, true);
    }
}
