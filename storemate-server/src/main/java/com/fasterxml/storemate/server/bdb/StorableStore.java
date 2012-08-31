package com.fasterxml.storemate.server.bdb;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.BufferRecycler;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.shared.WithBytesCallback;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;

import com.fasterxml.storemate.server.*;
import com.fasterxml.storemate.server.file.FileManager;
import com.fasterxml.storemate.server.file.FileReference;
import com.fasterxml.storemate.server.util.IOUtil;

import com.sleepycat.je.*;

/**
 * Simple abstraction for storing "decorated BLOBs", with a single
 * secondary index that can be used for traversing entries by
 * "last modified" time.
 */
public class StorableStore
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final KeyConverter KEY_CONV = new KeyConverter();
    
    /*
    /**********************************************************************
    /* Simple config, location
    /**********************************************************************
     */

    protected final File _dataRoot;

    /*
    /**********************************************************************
    /* Simple config, compression/inline settings
    /**********************************************************************
     */

    protected final boolean _compressionEnabled;
    protected final int _maxInlinedStorageSize;

    protected final int _minCompressibleSize;
    protected final int _maxGZIPCompressibleSize;

    protected final boolean _requireChecksumForPreCompressed;
    
    /*
    /**********************************************************************
    /* External helper objects
    /**********************************************************************
     */

    protected final TimeMaster _timeMaster;

    protected final FileManager _fileManager;

    protected final StorableConverter _storableConverter;

    /**
     * We can reuse read buffers as they are somewhat costly to
     * allocate, reallocate all the time. Buffer used needs to be big
     * enough to contain all conceivably inlineable cases (considering
     * possible compression).
     * Currently we'll use 64k as the cut-off point.
     */
    final private static BufferRecycler _bufferRecycler = new BufferRecycler(64000);
    
    /*
    /**********************************************************************
    /* BDB entities
    /**********************************************************************
     */

    /**
     * Underlying primary BDB-JE database
     */
    protected final Database _entries;

    /**
     * Secondary database that tracks last-modified order of primary entries.
     */
    protected final SecondaryDatabase _index;
    
    /*
    /**********************************************************************
    /* Store status
    /**********************************************************************
     */

    protected final AtomicBoolean _closed = new AtomicBoolean(false);

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public StorableStore(StoreConfig config, File dbRoot,
            TimeMaster timeMaster, FileManager fileManager,
            Database entryDB, SecondaryDatabase lastModIndex,
            StorableConverter conv)
    {
        _compressionEnabled = config.compressionEnabled;
        _minCompressibleSize = config.minUncompressedSizeForCompression;
        _maxGZIPCompressibleSize = config.maxUncompressedSizeForGZIP;
        _maxInlinedStorageSize = config.maxInlinedStorageSize;

        _requireChecksumForPreCompressed = config.requireChecksumForPreCompressed;

        _fileManager = fileManager;
        _timeMaster = timeMaster;
        _dataRoot = dbRoot;
        _entries = entryDB;
        _index = lastModIndex;
        _storableConverter = conv;
    }

    public void start()
    {
        // nothing to do, yet
    }
    
    public void stop()
    {
        if (!_closed.getAndSet(true)) {
            Environment env = _entries.getEnvironment();
            _index.close();
            _entries.close();
            env.close();
        }
    }
    
    /*
    /**********************************************************************
    /* API, simple accessors for state, helper objects
    /**********************************************************************
     */

    public boolean isClosed() {
        return _closed.get();
    }

    public FileManager getFileManager() {
        return _fileManager;
    }

    /*
    /**********************************************************************
    /* API, metadata access
    /**********************************************************************
     */

    /**
     * Accessor for getting approximate count of entries in the underlying
     * main BDB database
     */
    public long getEntryCount()
    {
        _checkClosed();
        return _entries.count();
    }

    /**
     * Accessor for getting approximate count of entries accessible
     * via last-modifed index.
     */
    public long getIndexedCount()
    {
        _checkClosed();
        return _entries.count();
    }
    
    /*
    /**********************************************************************
    /* API, data reads
    /**********************************************************************
     */

    public boolean hasEntry(StorableKey key)
    {
        _checkClosed();
        OperationStatus status = _entries.get(null, dbKey(key), null, LockMode.READ_COMMITTED);
        switch (status) {
        case SUCCESS:
        case KEYEXIST:
            return true;
        case KEYEMPTY: // was deleted during operation.. shouldn't be getting
        case NOTFOUND:
            // fall through
        }
        return false;
    }

    public Storable findEntry(StorableKey key) throws StoreException
    {
        _checkClosed();
        DatabaseEntry result = new DatabaseEntry();
        OperationStatus status = _entries.get(null, dbKey(key), result, LockMode.READ_COMMITTED);
        if (status != OperationStatus.SUCCESS) {
            return null;
        }
        return _storableConverter.decode(result.getData());
    }

    /*
    /**********************************************************************
    /* API, entry creation
    /**********************************************************************
     */
    
    /**
     * Method for inserting entry, <b>if and only if</b> no entry exists for
     * given key.
     * 
     * @param input Input stream used for reading the content. NOTE: method never
     *   closes this stream
     */
    public StorableCreationResult insert(StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, byte[] customMetadata)
        throws IOException, StoreException
    {
        return putEntry(key, input, stdMetadata, customMetadata, NO_OVERWRITES);
    }

    /**
     * Method for inserting entry, <b>if and only if</b> no entry exists for
     * given key.
     * 
     * @param input Input stream used for reading the content. NOTE: method never
     *   closes this stream
     */
    public StorableCreationResult putEntry(StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, byte[] customMetadata,
            OverwriteHandler overwriteHandler)
        throws IOException, StoreException
    {
        BufferRecycler.Holder bufferHolder = _bufferRecycler.getHolder();        
        final byte[] readBuffer = bufferHolder.borrowBuffer();
        int len = 0;

        try {
            try {
                len = IOUtil.readFully(input, readBuffer);
            } catch (IOException e) {
                throw new StoreException(key, "Failed to read payload for key "+key+": "+e.getMessage(), e);
            }
    
            // First things first: verify that compression is what it claims to be:
            final Compression originalCompression = stdMetadata.compression;
            String error = IOUtil.verifyCompression(originalCompression, readBuffer, len);
            if (error != null) {
                throw new StoreException(key, error);
            }
            if (len < readBuffer.length) { // read it all: we are done with input stream
                if (originalCompression == null) { // client did not compress, we may try to
                    return _compressAndPutSmallEntry(key, stdMetadata, customMetadata,
                            overwriteHandler, readBuffer, len);
                }
                return _putSmallPreCompressedEntry(key, stdMetadata, customMetadata,
                        overwriteHandler, readBuffer, len);
            }
            // partial read in buffer, rest from input stream:
            return _putLargeEntry(key, stdMetadata, customMetadata,
                    overwriteHandler, readBuffer, len, input);
        } finally {
            bufferHolder.returnBuffer(readBuffer);
        }
    }

    protected StorableCreationResult _compressAndPutSmallEntry(StorableKey key,
            StorableCreationMetadata metadata, byte[] customMetadata,
            OverwriteHandler overwriteHandler,
            byte[] readBuffer, int readByteCount)
        throws IOException, StoreException
    {
        final int origLength = readByteCount;
        // must verify checksum unless we got compressed payload
        // do we insist on checksum? Not if client has not yet compressed it:
        int actualChecksum = _calcChecksum(readBuffer, 0, readByteCount);
        final int origChecksum = metadata.contentHash;
        if (origChecksum == StoreConstants.NO_CHECKSUM) {
            metadata.contentHash = actualChecksum;
        } else {
            if (origChecksum != actualChecksum) {
                throw new StoreException(key, "Incorrect checksum (0x"+Integer.toHexString(origChecksum)
                        +"), calculated to be 0x"+Integer.toHexString(actualChecksum));
            }
        }
        if (_shouldTryToCompress(metadata, readBuffer, 0, origLength)) {
            byte[] compBytes;
            Compression compression = null;
            try {
                if (origLength <= _maxGZIPCompressibleSize) {
                    compression = Compression.GZIP;
                    compBytes = Compressors.gzipCompress(readBuffer, 0, origLength);
                } else {
                    compression = Compression.LZF;
                    compBytes = Compressors.lzfCompress(readBuffer, 0, origLength);
                }
            } catch (IOException e) {
                throw new StoreException(key, "Problem with compression ("+compression+"): "+e.getMessage(), e);
            }
            // if compression would just expand, don't use...
            if (compBytes.length >= origLength) {
                compression = null;
            } else {
                readBuffer = compBytes;
                readByteCount = compBytes.length;
                metadata.compressedContentHash = _calcChecksum(readBuffer, 0, readByteCount);
            }
        }
        return _putSmallEntry(key, metadata, customMetadata,
                overwriteHandler, readBuffer, readByteCount);
    }

    protected StorableCreationResult _putSmallPreCompressedEntry(StorableKey key,
            StorableCreationMetadata metadata, byte[] customMetadata,
            OverwriteHandler overwriteHandler,
            byte[] readBuffer, int readByteCount)
        throws IOException, StoreException
    {
        /* !!! TODO: what to do with checksum? Should we require checksum
         *   of raw or compressed entity? (or both); whether to store both;
         *   verify etc...
         */
        final int origChecksum = metadata.contentHash;
        if (origChecksum == StoreConstants.NO_CHECKSUM) {
            if (_requireChecksumForPreCompressed) {
                throw new StoreException(key,
                        "No checksum for non-compressed data provided for pre-compressed entry");
            }
        }

        // 30-Mar-2012, tsaloranta: Alas, we don't really know the length from gzip (and even
        //   from lzf would need to decode to some degree); not worth doing it
//        metadata.size = -1;
//        metadata.storageSize = dataLength;

        // may get checksum for compressed data, or might not; if not, calculate:
        if (metadata.compression != Compression.NONE) {
            if (metadata.compressedContentHash == StoreConstants.NO_CHECKSUM) {
                metadata.compressedContentHash = _calcChecksum(readBuffer, 0, readByteCount);
            }
        }
        return _putSmallEntry(key, metadata, customMetadata,
                overwriteHandler, readBuffer, readByteCount);
    }

    protected StorableCreationResult _putSmallEntry(StorableKey key,
            StorableCreationMetadata stdMetadata, byte[] customMetadata,
            OverwriteHandler overwriteHandler,
            byte[] readBuffer, int readByteCount)
        throws IOException, StoreException
    {
        Storable storable;
        final long now;

        // inline? Yes if small enough
        if (readByteCount <= _maxInlinedStorageSize) {
            now = _timeMaster.currentTimeMillis();
            storable = _storableConverter.encodeInlined(key, now,
                    stdMetadata, customMetadata, readBuffer, 0, readByteCount);
        } else {
            // otherwise, need to create file and all that fun...
            long fileCreationTime = _timeMaster.currentTimeMillis();
            FileReference fileRef = _fileManager.createStorageFile(key,
                    stdMetadata.compression, fileCreationTime);
            try {
                IOUtil.writeFile(fileRef.getFile(), readBuffer, 0, readByteCount);
            } catch (IOException e) {
                // better remove the file, if one exists...
                fileRef.getFile().delete();
                throw new StoreException(key,
                        "Failed to write storage file of "+readByteCount+" bytes: "+e.getMessage(), e);
            }
            // but modtime better be taken only now, as above may have taken some time (I/O bound)
            now = _timeMaster.currentTimeMillis();
            storable = _storableConverter.encodeOfflined(key, now,
                    stdMetadata, customMetadata, fileRef);
        }

        // Getting close: we have entry to store in BDB, if things work ok...

        // TODO: locking, check for overwrite...
        
        // since it may have taken 
        
        return null;
    }
        
    public StorableCreationResult _putLargeEntry(StorableKey key,
            StorableCreationMetadata metadata, byte[] customMetadata,
            OverwriteHandler overwriteHandler,
            byte[] readBuffer, int readByteCount,
            InputStream input)
        throws IOException, StoreException
    {
        return null;
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected int _calcChecksum(byte[] buffer, int offset, int length)
    {
        return BlockMurmur3Hasher.hash(0, buffer, offset, length);
    }
    
    /**
     * Helper method called to check whether given (partial) piece of content
     * might benefit from compression, as per currently defined rules.
     * To be eligible, all of below needs to be true:
     *<ul>
     * <li>compression is enabled for store
     * <li>caller indicated data isn't pre-compressed it (or indicate it does not want compression)
     * <li>data is big enough that it might help (i.e. it's not "too small to compress")
     * <li>data does not look like it has been compressed (regardless of what caller said) using
     *   one of algorithms we know of
     *</ul>
     */
    protected boolean _shouldTryToCompress(StorableCreationMetadata metadata,
            byte[] buffer, int offset, int length)
    {
        return _compressionEnabled
            && (metadata.compression == null)
            && (length >= _minCompressibleSize)
            && !Compressors.isCompressed(buffer, offset, length);
    }
    
    protected void _checkClosed()
    {
        if (_closed.get()) {
            throw new IllegalStateException("Can not access data from StorableStore after it has been closed");
        }
    }

    protected DatabaseEntry dbKey(StorableKey key)
    {
        return key.with(KEY_CONV);
    }
    
    private final static class KeyConverter implements WithBytesCallback<DatabaseEntry>
    {
        @Override
        public DatabaseEntry withBytes(byte[] buffer, int offset, int length) {
            if (offset == 0 && length == buffer.length) {
                return new DatabaseEntry(buffer);
            }
            return new DatabaseEntry(buffer, offset, length);
        }
    }

    /*
    /**********************************************************************
    /* Helper classes
    /**********************************************************************
     */

    private final static OverwriteHandler NO_OVERWRITES = new NoOverwrites();
    
    static class NoOverwrites implements OverwriteHandler
    {
        @Override
        public Response allowOverwrite(StorableKey key,
                StorableCreationMetadata metadata, Storable existingEntry)
                throws IOException, StoreException {
            return Response.LEAVE_AND_FAIL;
        }
    }

}
