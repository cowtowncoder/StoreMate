package com.fasterxml.storemate.store;

import java.io.File;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.util.IOUtil;
import com.fasterxml.storemate.shared.util.WithBytesCallback;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.impl.StorableFlags;

/**
 * Class that represents a single metadata entry read from the local database.
 * It is a relatively thin wrapper around raw {@link ByteContainer} value,
 * and its associated {@link StorableKey} key.
 */
public class Storable
{
   /**
    * Primary key of this instance, when stored in the data store
    */
    protected final StorableKey _key;

    protected final ByteContainer _rawEntry;
    
    /*
    /**********************************************************************
    /* Status metadata
    /**********************************************************************
     */

    protected final long _lastModified;

    protected final Compression _compression;

    protected final boolean _isDeleted;
    protected final boolean _isReplicated;

    @Override
    public String toString()
    {
        return "Storable: key="+_key+", rawLength="+_rawEntry.byteLength()
                +", compression="+_compression+", deleted="+_isDeleted
                +", extPathLen="+_externalPathLength
                ;
    }
    
    /*
    /**********************************************************************
    /* Metadata
    /**********************************************************************
     */
    
    protected final int _metadataOffset, _metadataLength;
    
    /*
    /**********************************************************************
    /* Content fields
    /**********************************************************************
     */
    
    // Murmur3/32 hash of uncompressed content, if available; 0 if not
    protected final int _contentHash;

    /**
     * Length of physical storage (inlined or offlined)
     */
    protected final long _storageLength;

    /**
     * Length of data before compression, iff compression used;
     * otherwise -1.
     */
    protected final long _originalLength;
    
    protected final int _compressedHash;

    protected final int _externalPathLength;

    /**
     * Pointer to either inlined data, or external path.
     *<p>
     * NOTE: payload section starts earlier, with 'storageLength';
     * this specifically points to actual payload data
     */
    protected final int _payloadOffset;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */
    
    public Storable(StorableKey key, ByteContainer bytes,
            long lastMod, int statusFlags,
            Compression comp, int externalPathLength,
            int contentHash, int compressedHash, long originalLength,
            int metadataOffset, int metadataLength,
            int payloadOffset, long storageLength)
    {
        _key = key;
        _rawEntry = bytes;

        _lastModified = lastMod;

        _isDeleted = (statusFlags & StorableFlags.F_STATUS_SOFT_DELETED) != 0;
        _isReplicated =  (statusFlags & StorableFlags.F_STATUS_REPLICATED) != 0;
        _compression = (comp == null) ? Compression.NONE : comp;
        _externalPathLength = externalPathLength;
        
        _contentHash = contentHash;
        _compressedHash = compressedHash;
        _originalLength = originalLength;
        
        _metadataOffset = metadataOffset;
        _metadataLength = metadataLength;
        
        _payloadOffset = payloadOffset;
        _storageLength = storageLength;

        // Sanity checking
        if (externalPathLength > 0) {
            if ((payloadOffset + externalPathLength) != bytes.byteLength()) {
                throw new IllegalStateException("Illegal Storable: payloadOffset ("+payloadOffset+") + extLen ("+ externalPathLength+") != total ("+bytes.byteLength()+")");
            }
        } else {
            if ((payloadOffset + storageLength) != bytes.byteLength()) {
                throw new IllegalStateException("Illegal Storable: payloadOffset ("+payloadOffset+") + storageLength ("+ storageLength+") != total ("+bytes.byteLength()+")");
            }
        }
    }

    /**
     * "Mutant factory" used by <code>StorableConverter</code> when simply
     * marking an entry as soft-deleted, without actually removing inlined
     * or external data.
     */
    public Storable softDeletedCopy(ByteContainer bytes, boolean removeData,
            long lastModTime)
    {
        // clear deleted flag, i.e. just retain replicated flag
        int statusFlags = StorableFlags.F_STATUS_SOFT_DELETED;
        if (_isReplicated) {
            statusFlags |= StorableFlags.F_STATUS_REPLICATED;
        }
        /* 09-Feb-2014, tatu: Since we may be clearing inlined data or
         *    external path, including removal of leading length prefix,
         *    payloadOffset may need to be truncated.
         */
        int payloadOffset = Math.min(_payloadOffset, bytes.byteLength());
        
        return new Storable(_key, bytes,
                lastModTime, statusFlags, _compression,
                removeData ? 0 : _externalPathLength,
                _contentHash, _compressedHash, _originalLength,
                _metadataOffset, _metadataLength,
                payloadOffset,
                removeData ? 0 : _storageLength);
    }
    
    /*
    /**********************************************************************
    /* API, accessors, simple/boolean
    /**********************************************************************
     */

    public StorableKey getKey() { return _key; }
    
    public long getLastModified() { return _lastModified; }
    public Compression getCompression() { return _compression; }

    public int getContentHash() { return _contentHash; }
    public int getCompressedHash() { return _compressedHash; }
    
    public long getStorageLength() { return _storageLength; }
    public long getOriginalLength() { return _originalLength; }
    public int getMetadataLength() { return _metadataLength; }
    public int getInlineDataLength() {
        if (_externalPathLength > 0L) {
            return 0;
        }
        return (int) _storageLength;
    }

    public boolean isDeleted() { return _isDeleted; }
    
    /**
     * Method to check whether this entry was created (or last updated) by
     * replication process; if not, it was created by directly. Exact
     * semantics depend on surrounding context, StoreMate simply notes
     * this setting.
     */
    public boolean isReplicated() { return _isReplicated; }

    public boolean hasInlineData() {
        return (_externalPathLength == 0L) && (_storageLength > 0L);
    }

    public boolean hasExternalData() {
        return (_externalPathLength > 0L);
    }

    /**
     * Accessor for getting length of the content, uncompressed if necessary.
     */
    public long getActualUncompressedLength() {
        if (_compression != Compression.NONE) {
            return _originalLength;
        }
        return _storageLength;
    }
    
    /*
    /**********************************************************************
    /* API, accessors for data
    /**********************************************************************
     */
    
    public File getExternalFile(FileManager mgr)
    {
        if (_externalPathLength <= 0) {
            return null;
        }
        ByteContainer extRef = _rawEntry.view(_payloadOffset, _externalPathLength);
        String filename = IOUtil.getLatin1String(extRef);
        return mgr.derefenceFile(filename);
    }

    /**
     * Accessor for getting relative path to external data File, if one exists;
     * or null if no external data used.
     */
    public String getExternalFilePath()
    {
        if (_externalPathLength <= 0) {
            return null;
        }
        return IOUtil.getLatin1String(_rawEntry.view(_payloadOffset, _externalPathLength));
    }
    
    public ByteContainer getMetadata() {
        if (_metadataLength <= 0) {
            return ByteContainer.emptyContainer();
        }
        return _rawEntry.view(_metadataOffset, _metadataLength);
    }

    public <T> T withMetadata(WithBytesCallback<T> cb) {
        if (_metadataLength <= 0) {
            return ByteContainer.emptyContainer().withBytes(cb);
        }
        return _rawEntry.withBytes(cb, _metadataOffset, _metadataLength);
    }

    public ByteContainer getInlinedData()
    {
        if (_externalPathLength > 0) {
            return ByteContainer.emptyContainer();
        }
        int len = (int) _storageLength;
        if (len <= 0) {
            return ByteContainer.emptyContainer();
        }
        return _rawEntry.view(_payloadOffset, len);
    }

    public <T> T withInlinedData(WithBytesCallback<T> cb)
    {
        if (_externalPathLength > 0) {
            return ByteContainer.emptyContainer().withBytes(cb);
        }
        int len = (int) _storageLength;
        if (len <= 0) {
            return ByteContainer.emptyContainer().withBytes(cb);
        }
        return _rawEntry.withBytes(cb, _payloadOffset, len);
    }
    
    /*
    /**********************************************************************
    /* API, access to raw serialization
    /**********************************************************************
     */

    public byte[] asBytes() {
        return _rawEntry.asBytes();
    }
    
    public <T> T withRaw(WithBytesCallback<T> cb) {
        return _rawEntry.withBytes(cb);
    }

    /**
     * Method for accessing content up to and including opaque metadata section,
     * but without payload section.
     */
    public <T> T withRawWithoutPayload(WithBytesCallback<T> cb) {
        final int len = _metadataOffset + _metadataLength;
        return _rawEntry.withBytes(cb, 0, len);
    }
}
