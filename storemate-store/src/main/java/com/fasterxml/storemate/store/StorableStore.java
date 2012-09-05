package com.fasterxml.storemate.store;

import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;
import com.fasterxml.storemate.shared.hash.IncrementalMurmur3Hasher;

import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileReference;
import com.fasterxml.storemate.store.util.CountingOutputStream;
import com.fasterxml.storemate.store.util.IOUtil;

/**
 * Simple abstraction for storing "decorated BLOBs", with a single
 * secondary index that can be used for traversing entries by
 * "last modified" time.
 */
public class StorableStore
{
    /**
     * No real seed used for Murmur3/32.
     */
    private final static int HASH_SEED = 0;

    /**
     * We will partition key space in 256 slices for locking purposes;
     * needs to be high enough to make lock contention very unlikely.
     */
    private final static int LOCK_PARTITIONS = 256;
    
    private final Logger LOG = LoggerFactory.getLogger(getClass());

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
    
    protected final PhysicalStore _physicalStore;

    /*
    /**********************************************************************
    /* Internal helper objects
    /**********************************************************************
     */
    
    /**
     * Helper object that knows how to encode and decode little bit of
     * metadata that we use.
     */
    protected final StorableConverter _storableConverter;

    /**
     * We will also need a simple form of locking to make 'read+write'
     * combinations atomic without requiring backend store to have
     * real transactions.
     * This is sufficient only because we know the specific usage pattern,
     * and the problem to resolve: it is not a general replacement for
     * real transactions.
     */
    protected final StorePartitions _partitions;
    
    /**
     * We can reuse read buffers as they are somewhat costly to
     * allocate, reallocate all the time. Buffer used needs to be big
     * enough to contain all conceivably inlineable cases (considering
     * possible compression).
     * Currently we'll use 64k as the cut-off point.
     */
    protected final static BufferRecycler _readBuffers = new BufferRecycler(64000);

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

    public StorableStore(StoreConfig config, PhysicalStore physicalStore,
            TimeMaster timeMaster, FileManager fileManager)
    {
        _compressionEnabled = config.compressionEnabled;
        _minCompressibleSize = config.minUncompressedSizeForCompression;
        _maxGZIPCompressibleSize = config.maxUncompressedSizeForGZIP;
        _maxInlinedStorageSize = config.maxInlinedStorageSize;
        
        _requireChecksumForPreCompressed = config.requireChecksumForPreCompressed;

        _physicalStore = physicalStore;
        _fileManager = fileManager;
        _timeMaster = timeMaster;
        _storableConverter = physicalStore.getStorableConverter();

        // May want to make this configurable in future...
        // 'true' means "fair", minor overhead, prevents potential starvation
        _partitions = new StorePartitions(_physicalStore, LOCK_PARTITIONS, true);
    }
    
    public void start()
    {
        _physicalStore.start();
    }
    
    public void stop()
    {
        if (!_closed.getAndSet(true)) {
            _physicalStore.stop();
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
        return _physicalStore.getEntryCount();
    }

    /**
     * Accessor for getting approximate count of entries accessible
     * via last-modifed index.
     */
    public long getIndexedCount()
    {
        _checkClosed();
        return _physicalStore.getIndexedCount();
    }
    
    /*
    /**********************************************************************
    /* API, data reads
    /**********************************************************************
     */

    public boolean hasEntry(StorableKey key)
    {
        _checkClosed();
        return _physicalStore.hasEntry(key);
    }

    public Storable findEntry(StorableKey key) throws StoreException
    {
        _checkClosed();
        return _physicalStore.findEntry(key);
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
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata)
        throws IOException, StoreException
    {
        return _putEntry(key, input, stdMetadata, customMetadata, false);
    }

    /**
     * Method for inserting entry, if no entry exists for the key, or updating
     * entry if one does. In case of update, results will contain information
     * about overwritten entry.
     * 
     * @param input Input stream used for reading the content. NOTE: method never
     *   closes this stream
     * @param removeOldDataFile Whether method should delete backing data file for
     *   the existing entry (if one was found) or not.
     */
    public StorableCreationResult upsert(StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile)
        throws IOException, StoreException
    {
        StorableCreationResult result = _putEntry(key, input, stdMetadata, customMetadata, false);
        if (removeOldDataFile) {
            Storable old = result.getPreviousEntry();
            if (old != null) {
                _deleteBackingFile(key, old.getExternalFile(_fileManager));
            }
        }
        return result;
    }
    
    /*
    /**********************************************************************
    /* Internal methods for entry creation
    /**********************************************************************
     */
    
    /**
     * Method for putting an entry in the database; depending on arguments, either
     * overwriting existing entry (if overwrites allowed), or failing insertion.
     * 
     * @param input Input stream used for reading the content. NOTE: method never
     *   closes this stream
     */
    protected StorableCreationResult _putEntry(StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean allowOverwrite)
        throws IOException, StoreException
    {
        BufferRecycler.Holder bufferHolder = _readBuffers.getHolder();        
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
                            allowOverwrite, readBuffer, len);
                }
                return _putSmallPreCompressedEntry(key, stdMetadata, customMetadata,
                        allowOverwrite, readBuffer, len);
            }
            // partial read in buffer, rest from input stream:
            return _putLargeEntry(key, stdMetadata, customMetadata,
                    allowOverwrite, readBuffer, len, input);
        } finally {
            bufferHolder.returnBuffer(readBuffer);
        }
    }
    
    protected StorableCreationResult _compressAndPutSmallEntry(StorableKey key,
            StorableCreationMetadata metadata, ByteContainer customMetadata,
            boolean allowOverwrite,
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
            // if compression would not, like, compress, don't bother:
            if (compBytes.length >= origLength) {
                compression = null;
            } else {
                readBuffer = compBytes;
                readByteCount = compBytes.length;
                metadata.uncompressedSize = origLength;
                metadata.compressedContentHash = _calcChecksum(readBuffer, 0, readByteCount);
            }
        }
        return _putSmallEntry(key, metadata, customMetadata,
                allowOverwrite, readBuffer, readByteCount);
    }

    protected StorableCreationResult _putSmallPreCompressedEntry(StorableKey key,
            StorableCreationMetadata metadata, ByteContainer customMetadata,
            boolean allowOverwrite,
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
                allowOverwrite, readBuffer, readByteCount);
    }

    protected StorableCreationResult _putSmallEntry(StorableKey key,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean allowOverwrite,
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
        return _putPartitionedEntry(key, stdMetadata, storable, allowOverwrite);
    }

    protected StorableCreationResult _putLargeEntry(StorableKey key,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean allowOverwrite,
            byte[] readBuffer, int readByteCount,
            InputStream input)
        throws IOException, StoreException
    {
        boolean skipCompression;
        Compression comp = stdMetadata.compression;

        if (comp != null) { // pre-compressed, or blocked
            skipCompression = true;
            comp = stdMetadata.compression;
        } else {
            if (!_compressionEnabled || Compressors.isCompressed(readBuffer, 0, readByteCount)) {
                skipCompression = true;
                comp = Compression.NONE;
            } else {
                skipCompression = false;
                comp = Compression.LZF;
            }
            stdMetadata.compression = comp;
        }
        
        // So: start by creating the result file
        long fileCreationTime = _timeMaster.currentTimeMillis();
        final FileReference fileRef = _fileManager.createStorageFile(key, comp, fileCreationTime);
        File storedFile = fileRef.getFile();
        
        OutputStream out;
        CountingOutputStream compressedOut;

        try {
            if (skipCompression) {
                compressedOut = null;
                out = new FileOutputStream(storedFile);
            } else {
                compressedOut = new CountingOutputStream(new FileOutputStream(storedFile),
                        new IncrementalMurmur3Hasher());
                out = Compressors.compressingStream(compressedOut, comp);
            }
            out.write(readBuffer, 0, readByteCount);
        } catch (IOException e) {
            throw new StoreException(key, "Failed to write initial "+readByteCount+" bytes of file '"+storedFile.getAbsolutePath()+"'", e);
        }

        IncrementalMurmur3Hasher hasher = new IncrementalMurmur3Hasher(HASH_SEED);        
        hasher.update(readBuffer, 0, readByteCount);
        long copiedBytes = readByteCount;
        
        // and then need to proceed with copying the rest, compressing along the way
        while (true) {
            int count;
            try {
                count = input.read(readBuffer);
            } catch (IOException e) { // probably will fail to write response too but...
                throw new StoreException(key, "Failed to read content to store (after "+copiedBytes+" bytes)", e);
            }
            if (count < 0) {
                break;
            }
            copiedBytes += count;
            try {
                out.write(readBuffer, 0, count);
            } catch (IOException e) {
                throw new StoreException(key, "Failed to write "+count+" bytes (after "+copiedBytes
                        +") to file '"+storedFile.getAbsolutePath()+"'", e);
            }
            hasher.update(readBuffer, 0, count);
        }
        try {
            out.close();
        } catch (IOException e) { }
        
        // Checksum calculation and storage details differ depending on whether compression is used
        if (skipCompression) {
            final int actualHash = hasher.calculateHash();
            stdMetadata.storageSize = copiedBytes;
            if (stdMetadata.compression == Compression.NONE) {
                if (stdMetadata.contentHash == StoreConstants.NO_CHECKSUM) {
                    stdMetadata.contentHash = actualHash;
                } else if (stdMetadata.contentHash != actualHash) {
                    throw new StoreException(key, "Incorrect checksum for entry ("+copiedBytes+" bytes): got 0x"
                                    +Integer.toHexString(stdMetadata.contentHash)+", calculated to be 0x"
                                    +Integer.toHexString(actualHash));
                }
            } else { // already compressed
//                stdMetadata.compressedContentHash = hasher.calculateHash();
                if (stdMetadata.compressedContentHash == StoreConstants.NO_CHECKSUM) {
                    stdMetadata.compressedContentHash = actualHash;
                } else {
                    if (stdMetadata.compressedContentHash != actualHash) {
                        throw new StoreException(key, "Incorrect checksum for entry ("+copiedBytes+" bytes): got 0x"
                                        +Integer.toHexString(stdMetadata.compressedContentHash)+", calculated to be 0x"
                                        +Integer.toHexString(actualHash));
                    }
                }
            }
        } else {
            final int contentHash = hasher.calculateHash();
            final int compressedHash = compressedOut.calculateHash();

            stdMetadata.uncompressedSize = copiedBytes;
            stdMetadata.storageSize = compressedOut.count();
            // must verify checksum, if one was offered...
            if (stdMetadata.contentHash == StoreConstants.NO_CHECKSUM) {
                stdMetadata.contentHash = contentHash;
            } else {
                if (stdMetadata.contentHash != contentHash) {
                    throw new StoreException(key, "Incorrect checksum for entry ("+copiedBytes+" bytes): got 0x"
                                    +Integer.toHexString(stdMetadata.contentHash)+", calculated to be 0x"
                                    +Integer.toHexString(contentHash));
                }
            }
            if (stdMetadata.compressedContentHash == StoreConstants.NO_CHECKSUM) {
                stdMetadata.compressedContentHash = compressedHash;
            } else {
                if (stdMetadata.compressedContentHash != compressedHash) {
                    throw new StoreException(key, "Incorrect checksum for compressed entry ("+stdMetadata.storageSize+"/"+copiedBytes
                                +" bytes): got 0x"
                                +Integer.toHexString(stdMetadata.compressedContentHash)+", calculated to be 0x"
                                +Integer.toHexString(compressedHash));
                }
            }
        }
        long now = _timeMaster.currentTimeMillis();
        Storable storable = _storableConverter.encodeOfflined(key, now,
                stdMetadata, customMetadata, fileRef);

        return _putPartitionedEntry(key, stdMetadata, storable, allowOverwrite);
    }

    protected StorableCreationResult _putPartitionedEntry(StorableKey key,
                StorableCreationMetadata stdMetadata, Storable storable,
                boolean allowOverwrite)
        throws IOException, StoreException
    {
        StorableCreationResult result = _partitions.put(key, stdMetadata, storable, allowOverwrite);
        if (!result.succeeded()) {
            // One piece of clean up: for failed insert, delete backing file, if any
            if (!allowOverwrite) {
                // otherwise, may need to delete file that was created
                FileReference ref = stdMetadata.dataFile;
                if (ref != null) {
                    _deleteBackingFile(key, ref.getFile());
                }
            }
        }
        return result;
    }
    
    /*
    /**********************************************************************
    /* Internal methods, other
    /**********************************************************************
     */

    protected void _deleteBackingFile(StorableKey key, File extFile)
    {
        if (extFile != null) {
            try {
                boolean ok = extFile.delete();
                if (!ok) {
                    LOG.warn("Failed to delete backing data file of key {}, path: {}",
                            key, extFile.getAbsolutePath());
                }
            } catch (Exception e) {
                LOG.warn("Failed to delete backing data file of key "+key+", path: "+extFile.getAbsolutePath(), e);
            }
        }
    }    
            
    protected int _calcChecksum(byte[] buffer, int offset, int length)
    {
        return BlockMurmur3Hasher.hash(HASH_SEED, buffer, offset, length);
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
}
