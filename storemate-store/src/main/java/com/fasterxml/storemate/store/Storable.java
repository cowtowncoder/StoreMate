package com.fasterxml.storemate.store;

import java.io.File;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.IOUtil;
import com.fasterxml.storemate.shared.WithBytesCallback;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.file.FileManager;

/**
 * Class that represents an entry read from the backing BDB-JE store.
 */
public class Storable
{
    protected final ByteContainer _rawEntry;

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
    
    public Storable(ByteContainer bytes,
            long lastMod,
            boolean isDeleted, Compression comp, int externalPathLength,
            int contentHash, int compressedHash, long originalLength,
            int metadataOffset, int metadataLength,
            int payloadOffset, long storageLength)
    {
        _rawEntry = bytes;

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
        ByteContainer extRef = _rawEntry.view(_payloadOffset, _externalPathLength);
        return mgr.derefenceFile(IOUtil.getAsciiString(extRef));
    }
    
    /*
    /**********************************************************************
    /* API, access to raw serialization
    /**********************************************************************
     */

    public <T> T withRaw(WithBytesCallback<T> cb) {
        return _rawEntry.withBytes(cb);
    }
}
