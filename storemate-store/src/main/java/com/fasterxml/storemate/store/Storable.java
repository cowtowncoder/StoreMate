package com.fasterxml.storemate.store;

import java.io.File;

import com.fasterxml.storemate.shared.WithBytesCallback;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.util.IOUtil;

/**
 * Class that represents an entry read from the backing BDB-JE store.
 */
public class Storable
{
    /**
     * Lazily populated copy of raw data from within entry
     */
    protected final byte[] _rawEntry;

    protected final int _rawOffset;
    
    protected final int _rawLength;

    /*
    /**********************************************************************
    /* Status metadata
    /**********************************************************************
     */

    protected final long _lastModified;

    protected final Compression _compression;
    
    protected final boolean _isDeleted;

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
     * Pointer to either inlined data, or external path
     */
    protected final int _payloadOffset;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */
    
    public Storable(byte[] raw, int rawOffset, int rawLength,
            long lastMod,
            boolean isDeleted, Compression comp, int externalPathLength,
            int contentHash, int compressedHash, long originalLength,
            int metadataOffset, int metadataLength,
            int payloadOffset, long storageLength)
    {
        _rawEntry = raw;
        _rawOffset = rawOffset;
        _rawLength = rawLength;

        _lastModified = lastMod;

        _isDeleted = isDeleted;
        _compression = comp;
        _externalPathLength = externalPathLength;
        
        _contentHash = contentHash;
        _compressedHash = compressedHash;
        _originalLength = originalLength;
        
        _metadataOffset = metadataOffset;
        _metadataLength = metadataLength;
        
        _payloadOffset = payloadOffset;
        _storageLength = storageLength;
    }

    /*
    /**********************************************************************
    /* API, accessors
    /**********************************************************************
     */

    public boolean isDeleted() { return _isDeleted; }

    public File getExternalFile(FileManager mgr)
    {
        if (_externalPathLength <= 0) {
            return null;
        }
        return mgr.derefenceFile(IOUtil.getAsciiString(_rawEntry, _payloadOffset, _externalPathLength));
    }
    
    /*
    /**********************************************************************
    /* API, access to raw serialization
    /**********************************************************************
     */

    public <T> T withRaw(WithBytesCallback<T> cb) {
        return cb.withBytes(_rawEntry, _rawOffset, _rawLength);
    }
}
