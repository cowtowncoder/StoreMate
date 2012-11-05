package com.fasterxml.storemate.store.impl;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.BlockHasher32;
import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;
import com.fasterxml.storemate.shared.hash.IncrementalMurmur3Hasher;
import com.fasterxml.storemate.shared.util.BufferRecycler;
import com.fasterxml.storemate.shared.util.IOUtil;
import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.IterationResult;
import com.fasterxml.storemate.store.backend.StorableIterationCallback;
import com.fasterxml.storemate.store.backend.StorableLastModIterationCallback;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileReference;
import com.fasterxml.storemate.store.util.CountingOutputStream;

/**
 * Full store front-end implementation.
 */
public class StorableStoreImpl extends AdminStorableStore
{
    /**
     * No real seed used for Murmur3/32.
     */
    private final static int HASH_SEED = BlockHasher32.DEFAULT_SEED;

    /**
     * We will partition key space in 64 slices for locking purposes;
     * needs to be high enough to make lock contention unlikely, but
     * shouldn't be too high to waste resources on locks themselves.
     */
    private final static int LOCK_PARTITIONS = 64;
    
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

    protected final int _minBytesToStream;
    
    protected final boolean _requireChecksumForPreCompressed;
    
    /*
    /**********************************************************************
    /* External helper objects
    /**********************************************************************
     */

    protected final TimeMaster _timeMaster;

    protected final FileManager _fileManager;
    
    /**
     * Backend store implementation that abstracts out differences between
     * underlying physical storage libraries.
     */
    protected final StoreBackend _backend;

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
    protected final static BufferRecycler _readBuffers = new BufferRecycler(StoreConfig.DEFAULT_MIN_PAYLOAD_FOR_STREAMING);

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

    public StorableStoreImpl(StoreConfig config, StoreBackend physicalStore,
            TimeMaster timeMaster, FileManager fileManager)
    {
        _compressionEnabled = config.compressionEnabled;
        _minCompressibleSize = config.minUncompressedSizeForCompression;
        _maxGZIPCompressibleSize = config.maxUncompressedSizeForGZIP;
        _maxInlinedStorageSize = config.maxInlinedStorageSize;
        _minBytesToStream = config.minPayloadForStreaming;
        
        _requireChecksumForPreCompressed = config.requireChecksumForPreCompressed;

        _backend = physicalStore;
        _fileManager = fileManager;
        _timeMaster = timeMaster;
        _storableConverter = physicalStore.getStorableConverter();

        // May want to make this configurable in future...
        // 'true' means "fair", minor overhead, prevents potential starvation
        _partitions = new StorePartitions(_backend, LOCK_PARTITIONS, true);
    }

    @Override
    public void start()
    {
        _backend.start();
    }
    
    @Override
    public void stop()
    {
        if (!_closed.getAndSet(true)) {
            _backend.stop();
        }
    }
    
    /*
    /**********************************************************************
    /* API, simple accessors for state, helper objects
    /**********************************************************************
     */

    @Override
    public boolean isClosed() {
        return _closed.get();
    }

    @Override
    public FileManager getFileManager() {
        return _fileManager;
    }

    @Override
    public TimeMaster getTimeMaster() {
        return _timeMaster;
    }
    
    /*
    /**********************************************************************
    /* API, metadata access
    /**********************************************************************
     */

    @Override
    public long getEntryCount()
    {
        _checkClosed();
        return _backend.getEntryCount();
    }

    @Override
    public long getIndexedCount()
    {
        _checkClosed();
        return _backend.getIndexedCount();
    }

    @Override
    public long getOldestInFlightTimestamp() {
        return _partitions.getOldestInFlightTimestamp();
    }
    
    /*
    /**********************************************************************
    /* API, data reads
    /**********************************************************************
     */

    @Override
    public boolean hasEntry(StorableKey key)
    {
        _checkClosed();
        return _backend.hasEntry(key);
    }

    @Override
    public Storable findEntry(StorableKey key) throws StoreException
    {
        _checkClosed();
        return _backend.findEntry(key);
    }

    /*
    /**********************************************************************
    /* API, entry creation
    /**********************************************************************
     */
    
    @Override
    public StorableCreationResult insert(StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata)
        throws IOException, StoreException
    {
        _checkClosed();
        return _putEntry(key, input, stdMetadata, customMetadata, false);
    }

    @Override
    public StorableCreationResult insert(StorableKey key, ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata)
        throws IOException, StoreException
    {
        _checkClosed();
        return _putEntry(key, input, stdMetadata, customMetadata, false);
    }
    
    @Override
    public StorableCreationResult upsert(StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile)
        throws IOException, StoreException
    {
        _checkClosed();
        StorableCreationResult result = _putEntry(key, input, stdMetadata, customMetadata, false);
        if (removeOldDataFile) {
            Storable old = result.getPreviousEntry();
            if (old != null) {
                _deleteBackingFile(key, old.getExternalFile(_fileManager));
            }
        }
        return result;
    }

    @Override
    public StorableCreationResult upsert(StorableKey key, ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile)
        throws IOException, StoreException
    {
        _checkClosed();
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
    /* Internal methods for entry creation, first level
    /**********************************************************************
     */
    
    /**
     * Method for putting an entry in the database; depending on arguments, either
     * overwriting existing entry (if overwrites allowed), or failing insertion.
     * 
     * @param stdMetadata Standard metadata, which <b>may be modified</b> by this
     *   method, to "fill in" optional or missing data.
     * @param input Input stream used for reading the content. NOTE: method never
     *   closes this stream
     */
    protected StorableCreationResult _putEntry(StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean allowOverwrite)
        throws IOException, StoreException
    {
    	/* NOTE: we do NOT want to clone passed-in metadata, because we want
    	 * to fill in some of optional values, and override others (compression)
    	 */
        BufferRecycler.Holder bufferHolder = _readBuffers.getHolder();        
        final byte[] readBuffer = bufferHolder.borrowBuffer(_minBytesToStream);
        int len = 0;

        try {
            try {
                len = IOUtil.readFully(input, readBuffer);
            } catch (IOException e) {
                throw new StoreException.IO(key, "Failed to read payload for key "+key+": "+e.getMessage(), e);
            }
    
            // First things first: verify that compression is what it claims to be:
            final Compression originalCompression = stdMetadata.compression;
            String error = IOUtil.verifyCompression(originalCompression, readBuffer, len);
            if (error != null) {
                throw new StoreException.Input(key, StoreException.InputProblem.BAD_COMPRESSION, error);
            }
            if (len < readBuffer.length) { // read it all: we are done with input stream
                if (originalCompression == null) { // client did not compress, we may try to
                    return _compressAndPutSmallEntry(key, stdMetadata, customMetadata,
                            allowOverwrite, ByteContainer.simple(readBuffer, 0, len));
                }
                return _putSmallPreCompressedEntry(key, stdMetadata, customMetadata,
                        allowOverwrite, ByteContainer.simple(readBuffer, 0, len));
            }
            // partial read in buffer, rest from input stream:
            return _putLargeEntry(key, stdMetadata, customMetadata,
                    allowOverwrite, readBuffer, len, input);
        } finally {
            bufferHolder.returnBuffer(readBuffer);
        }
    }

    protected StorableCreationResult _putEntry(StorableKey key, ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean allowOverwrite)
        throws IOException, StoreException
    {
        // First things first: verify that compression is what it claims to be:
        final Compression originalCompression = stdMetadata.compression;
        String error = IOUtil.verifyCompression(originalCompression, input);
        if (error != null) {
            throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM, error);
        }
        if (originalCompression == null) { // client did not compress, we may try to
            return _compressAndPutSmallEntry(key, stdMetadata, customMetadata,
                    allowOverwrite, input);
        }
        return _putSmallPreCompressedEntry(key, stdMetadata, customMetadata,
                allowOverwrite, input);
    }
    
    /*
    /**********************************************************************
    /* Internal methods for entry creation, second level
    /**********************************************************************
     */
    
    protected StorableCreationResult _compressAndPutSmallEntry(StorableKey key,
            StorableCreationMetadata metadata, ByteContainer customMetadata,
            boolean allowOverwrite, ByteContainer data)
        throws IOException, StoreException
    {
        final int origLength = data.byteLength();
        // must verify checksum unless we got compressed payload
        // do we insist on checksum? Not if client has not yet compressed it:
        int actualChecksum = _calcChecksum(data);
        final int origChecksum = metadata.contentHash;
        if (origChecksum == StoreConstants.NO_CHECKSUM) {
            metadata.contentHash = actualChecksum;
        } else {
            if (origChecksum != actualChecksum) {
                throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM,
                        "Incorrect checksum (0x"+Integer.toHexString(origChecksum)
                        +"), calculated to be 0x"+Integer.toHexString(actualChecksum));
            }
        }
        if (_shouldTryToCompress(metadata, data)) {
            byte[] compBytes;
            Compression compression = null;
            try {
                if (origLength <= _maxGZIPCompressibleSize) {
                    compression = Compression.GZIP;
                    compBytes = Compressors.gzipCompress(data);
                } else {
                    compression = Compression.LZF;
                    compBytes = Compressors.lzfCompress(data);
                }
            } catch (IOException e) {
                throw new StoreException.IO(key,
                        "Problem when compressing content as "+compression+": "+e.getMessage(), e);
            }
            // if compression would not, like, compress, don't bother:
            if (compBytes.length >= origLength) {
                compression = null;
            } else {
                data = ByteContainer.simple(compBytes);
                metadata.compression = compression;
                metadata.uncompressedSize = origLength;
                metadata.storageSize = compBytes.length;
                metadata.compressedContentHash = _calcChecksum(data);
            }
        }
        metadata.storageSize = data.byteLength();
        return _putSmallEntry(key, metadata, customMetadata, allowOverwrite, data);
    }

    protected StorableCreationResult _putSmallPreCompressedEntry(StorableKey key,
            StorableCreationMetadata metadata, ByteContainer customMetadata,
            boolean allowOverwrite, ByteContainer data)
        throws IOException, StoreException
    {
        /* !!! TODO: what to do with checksum? Should we require checksum
         *   of raw or compressed entity? (or both); whether to store both;
         *   verify etc...
         */
        final int origChecksum = metadata.contentHash;
        if (origChecksum == StoreConstants.NO_CHECKSUM) {
            if (_requireChecksumForPreCompressed) {
                throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM,
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
                metadata.compressedContentHash = _calcChecksum(data);
            }
        }
        return _putSmallEntry(key, metadata, customMetadata, allowOverwrite, data);
    }

    protected StorableCreationResult _putSmallEntry(StorableKey key,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean allowOverwrite, ByteContainer data)
        throws IOException, StoreException
    {
        Storable storable;
        final long creationTime;
        
        // inline? Yes if small enough
        if (data.byteLength() <= _maxInlinedStorageSize) {
            creationTime = _timeMaster.currentTimeMillis();
            storable = _storableConverter.encodeInlined(key, creationTime,
                    stdMetadata, customMetadata, data);
        } else {
            // otherwise, need to create file and all that fun...
            long fileCreationTime = _timeMaster.currentTimeMillis();
            FileReference fileRef = _fileManager.createStorageFile(key,
                    stdMetadata.compression, fileCreationTime);
            try {
                IOUtil.writeFile(fileRef.getFile(), data);
            } catch (IOException e) {
                // better remove the file, if one exists...
                fileRef.getFile().delete();
                throw new StoreException.IO(key,
                        "Failed to write storage file of "+data.byteLength()+" bytes: "+e.getMessage(), e);
            }
            // but modtime better be taken only now, as above may have taken some time (I/O bound)
            creationTime = _timeMaster.currentTimeMillis();
            storable = _storableConverter.encodeOfflined(key, creationTime,
                    stdMetadata, customMetadata, fileRef);
        }
        return _putPartitionedEntry(key, creationTime, stdMetadata, storable, allowOverwrite);
    }

    @SuppressWarnings("resource")
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
        
        OutputStream out = null;
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
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e2) { }
            throw new StoreException.IO(key, "Failed to write initial "+readByteCount+" bytes of file '"+storedFile.getAbsolutePath()+"'", e);
        }
        IncrementalMurmur3Hasher hasher = new IncrementalMurmur3Hasher(HASH_SEED);        
        hasher.update(readBuffer, 0, readByteCount);
        long copiedBytes = readByteCount;
        
        // and then need to proceed with copying the rest, compressing along the way
        try {
            while (true) {
                int count;
                try {
                    count = input.read(readBuffer);
                } catch (IOException e) { // probably will fail to write response too but...
                    throw new StoreException.IO(key, "Failed to read content to store (after "+copiedBytes+" bytes)", e);
                }
                if (count < 0) {
                    break;
                }
                copiedBytes += count;
                try {
                    out.write(readBuffer, 0, count);
                } catch (IOException e) {
                    throw new StoreException.IO(key, "Failed to write "+count+" bytes (after "+copiedBytes
                            +") to file '"+storedFile.getAbsolutePath()+"'", e);
                }
                hasher.update(readBuffer, 0, count);
            }
        } finally {
            try {
                out.close();
            } catch (IOException e) { }
        }
        
        // Checksum calculation and storage details differ depending on whether compression is used
        if (skipCompression) {
            // Storage sizes must match, first of all, if provided
            if (stdMetadata.storageSize != copiedBytes && stdMetadata.storageSize >= 0) {
                throw new StoreException.Input(key, StoreException.InputProblem.BAD_LENGTH,
                        "Incorrect length for entry; storageSize="+stdMetadata.storageSize
                        +", bytes read: "+copiedBytes);
            }

        	final int actualHash = hasher.calculateHash();
            stdMetadata.storageSize = copiedBytes;
            if (stdMetadata.compression == Compression.NONE) {
                if (stdMetadata.contentHash == StoreConstants.NO_CHECKSUM) {
                    stdMetadata.contentHash = actualHash;
                } else if (stdMetadata.contentHash != actualHash) {
                    throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM,
                            "Incorrect checksum for not-compressed entry ("+copiedBytes+" bytes): got 0x"
                                    +Integer.toHexString(stdMetadata.contentHash)+", calculated to be 0x"
                                    +Integer.toHexString(actualHash));
                }
            } else { // already compressed
//                stdMetadata.compressedContentHash = hasher.calculateHash();
                if (stdMetadata.compressedContentHash == StoreConstants.NO_CHECKSUM) {
                    stdMetadata.compressedContentHash = actualHash;
                } else {
                    if (stdMetadata.compressedContentHash != actualHash) {
                        throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM,
                                "Incorrect checksum for "+stdMetadata.compression+" pre-compressed entry ("+copiedBytes
                                +" bytes): got 0x"
                                +Integer.toHexString(stdMetadata.compressedContentHash)+", calculated to be 0x"
                                +Integer.toHexString(actualHash));
                    }
                }
            }
            // we don't really know the original size, either way:
            stdMetadata.uncompressedSize = 0L;
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
                    throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM,
                            "Incorrect checksum for entry ("+copiedBytes+" bytes, compression: "
                            		+stdMetadata.compression+"; comp checksum 0x"+stdMetadata.compressedContentHash
                            		+"): got 0x"+Integer.toHexString(stdMetadata.contentHash)
                            		+", calculated to be 0x"+Integer.toHexString(contentHash));
                }
            }
            if (stdMetadata.compressedContentHash == StoreConstants.NO_CHECKSUM) {
                stdMetadata.compressedContentHash = compressedHash;
            } else {
                if (stdMetadata.compressedContentHash != compressedHash) {
                    throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM,
                            "Incorrect checksum for "+stdMetadata.compression+" compressed entry ("
                            		+stdMetadata.storageSize+"/"+copiedBytes+" bytes): got 0x"
                                +Integer.toHexString(stdMetadata.compressedContentHash)+", calculated to be 0x"
                                +Integer.toHexString(compressedHash));
                }
            }
        }
        long creationTime = _timeMaster.currentTimeMillis();
        Storable storable = _storableConverter.encodeOfflined(key, creationTime,
                stdMetadata, customMetadata, fileRef);

        return _putPartitionedEntry(key, creationTime, stdMetadata, storable, allowOverwrite);
    }

    /**
     * @param operationTime Timestamp used as the "last-modified" timestamp in metadata;
     *   important as it determines last-modified traversal order for synchronization
     */
    protected StorableCreationResult _putPartitionedEntry(StorableKey key,
            final long operationTime,
            final StorableCreationMetadata stdMetadata, Storable storable,
            final boolean allowOverwrite)
        throws IOException, StoreException
    {
        StorableCreationResult result = _partitions.withLockedPartition(key, operationTime,
                new StoreOperationCallback<Storable,StorableCreationResult>() {
                    @Override
                    public StorableCreationResult perform(StorableKey k0,
                            StoreBackend backend, Storable s0)
                        throws IOException, StoreException
                    {
                        if (allowOverwrite) { // "upsert"
                            Storable old = backend.putEntry(k0, s0);
                            return new StorableCreationResult(k0, true, s0, old);
                        }
                        // strict "insert"
                        Storable old = backend.createEntry(k0, s0);
                        if (old == null) { // ok, succeeded
                            return new StorableCreationResult(k0, true, s0, null);
                        }
                        // fail: caller may need to clean up the underlying file
                        return new StorableCreationResult(k0, false, s0, old);
                    }
                },
                storable);

        //_partitions.put(key, stdMetadata, storable, allowOverwrite);
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
    /* API, entry deletion
    /**********************************************************************
     */

    @Override
    public StorableDeletionResult softDelete(StorableKey key,
            final boolean removeInlinedData, final boolean removeExternalData)
        throws IOException, StoreException
    {
        _checkClosed();
        final long currentTime = _timeMaster.currentTimeMillis();
        Storable entry = _partitions.withLockedPartition(key, currentTime,
            new ReadModifyOperationCallback<Object,Storable>() {
                @Override
                protected Storable perform(StorableKey k0,
                        StoreBackend backend, Object arg, Storable e0)
                    throws IOException, StoreException
                {
                    // First things first: if no entry, nothing to do
                    if (e0 == null) {
                        return null;
                    }
                    return _softDelete(k0, e0, currentTime, removeInlinedData, removeExternalData);
                }
        }, null);
        return new StorableDeletionResult(key, entry);
    }
    
    @Override
    public StorableDeletionResult hardDelete(StorableKey key,
            final boolean removeExternalData)
        throws IOException, StoreException
    {
        _checkClosed();
        final long currentTime = _timeMaster.currentTimeMillis();
        Storable entry = _partitions.withLockedPartition(key, currentTime,
            new ReadModifyOperationCallback<Object,Storable>() {

                @Override
                protected Storable perform(StorableKey k0,
                        StoreBackend backend, Object arg, Storable e0)
                    throws IOException, StoreException
                {                
                    // First things first: if no entry, nothing to do
                    if (e0 == null) {
                        return null;
                    }
                    return _hardDelete(k0, e0, removeExternalData);
                }
        }, null);
        return new StorableDeletionResult(key, entry);
    }

    /*
    /**********************************************************************
    /* API, public entry iteration methods
    /**********************************************************************
     */
    
    @Override
    public IterationResult iterateEntriesByKey(StorableIterationCallback cb,
            StorableKey firstKey)
        throws StoreException
    {
        return _backend.iterateEntriesByKey(cb, firstKey);
    }

    @Override
    public IterationResult iterateEntriesByModifiedTime(StorableLastModIterationCallback cb,
            long firstTimestamp)
        throws StoreException
    {
        return _backend.iterateEntriesByModifiedTime(cb, firstTimestamp);
    }
    
    /*
    /**********************************************************************
    /* API, admin methods (from AdminStorableStore) 
    /**********************************************************************
     */

    @Override
    public int getInFlightWritesCount()
    {
        return _partitions.getInFlightCount();
    }
    
    @Override
    public long getTombstoneCount(long maxRuntimeMsecs)
        throws StoreException
    {
        final long startTime = _timeMaster.currentTimeMillis();
        final long maxMax = Long.MAX_VALUE - startTime;
        final long maxEndTime = startTime + Math.min(maxMax, maxRuntimeMsecs);

        TombstoneCounter counter = new TombstoneCounter(_timeMaster, maxEndTime);
        if (_backend.scanEntries(counter) != IterationResult.FULLY_ITERATED) {
            throw new IllegalStateException("getTombstoneCount() run too long (max "+maxRuntimeMsecs
                    +"); failed after "+counter.tombstones+"/"+counter.total+" records");
        }
        return counter.tombstones;
    }

    @Override
    public List<Storable> dumpEntries(final int maxCount, final boolean includeDeleted)
        throws StoreException
    {
        final ArrayList<Storable> result = new ArrayList<Storable>();
        if (maxCount > 0) {
            _backend.iterateEntriesByKey(new StorableIterationCallback() {
                // all keys are fine
                @Override public IterationAction verifyKey(StorableKey key) { return IterationAction.PROCESS_ENTRY; }
                @Override
                public IterationAction processEntry(Storable entry) {
                    if (includeDeleted || !entry.isDeleted()) {
                        result.add(entry);
                        if (result.size() >= maxCount) {
                            return IterationAction.TERMINATE_ITERATION;
                        }
                    }
                    return IterationAction.PROCESS_ENTRY;
                }
            });
        }
        return result;
    }

    /**
     * Method for iterating over entries in creation-time order,
     * from the oldest to newest entries.
     */
    @Override
    public List<Storable> dumpOldestEntries(final int maxCount,
            final long fromTime, final boolean includeDeleted)
        throws StoreException
    {
        final ArrayList<Storable> result = new ArrayList<Storable>();
        if (maxCount > 0) {
            _backend.iterateEntriesByModifiedTime(new StorableLastModIterationCallback() {
                // we are fine with all timestamps
                @Override
                public IterationAction verifyTimestamp(long timestamp) {
                    return IterationAction.PROCESS_ENTRY;
                }
                // all keys are fine
                @Override public IterationAction verifyKey(StorableKey key) {
                    return IterationAction.PROCESS_ENTRY;
                }
                @Override
                public IterationAction processEntry(Storable entry) {
                    if (includeDeleted || !entry.isDeleted()) {
                        result.add(entry);
                        if (result.size() >= maxCount) {
                            return IterationAction.TERMINATE_ITERATION;
                        }
                    }
                    return IterationAction.PROCESS_ENTRY;
                }
            }, fromTime);
        }
        return result;
    }

    /**
     * Method for physically deleting specified number of entries, in
     * whatever order entries are stored in the database (not necessarily
     * insertion order)
     * 
     * @return Number of entries deleted
     */
    @Override
    public int removeEntries(final int maxToRemove)
        throws IOException, StoreException
    {
        int removed = 0;
        if (maxToRemove > 0) {
            StorableCollector collector = new StorableCollector(maxToRemove) {
                @Override
                public boolean includeEntry(Storable entry) { // any and all entries
                    return true;
                }
            };
            for (StorableKey key : collector.getCollected()) {
                hardDelete(key, true);
                ++removed;
            }
        }
        return removed;
    }

    /**
     * Helper method only to be called by tests; normal operation should
     * rely on background tombstone cleaning process.
     * 
     * @param maxToRemove Max number of tombstones to delete
     * 
     * @return Number of tombstones actually deleted
     */
    @Override
    public int removeTombstones(final int maxToRemove)
        throws IOException, StoreException
    {
        int removed = 0;
        if (maxToRemove > 0) {
            StorableCollector collector = new StorableCollector(maxToRemove) {
                @Override
                public boolean includeEntry(Storable entry) {
                    return entry.isDeleted();
                }
            };
            /* no time limit on tombstone removal. But should we scan (unordered)
             * or iterate?
             */
            _backend.iterateEntriesByKey(collector);
            for (StorableKey key : collector.getCollected()) {
                hardDelete(key, true);
                ++removed;
            }
        }
        return removed;
    }
    
    /*
    /**********************************************************************
    /* Internal methods for entry deletion
    /**********************************************************************
     */
    
    protected Storable _softDelete(StorableKey key, Storable entry, final long currentTime,
            final boolean removeInlinedData, final boolean removeExternalData)
        throws IOException, StoreException
    {
        // Ok now... need to delete some data?
        boolean hasExternalToDelete = removeExternalData && entry.hasExternalData();
        if (!entry.isDeleted() || hasExternalToDelete
                || (removeInlinedData && entry.hasInlineData())) {
            File extFile = hasExternalToDelete ? entry.getExternalFile(_fileManager) : null;
            Storable modEntry = _storableConverter.softDeletedCopy(key, entry, currentTime,
                    removeInlinedData, removeExternalData);
            _backend.ovewriteEntry(key, modEntry);
            if (extFile != null) {
                _deleteBackingFile(key, extFile);
            }
            return modEntry;
        }
        return entry;
    }

    protected Storable _hardDelete(StorableKey key, Storable entry,
            final boolean removeExternalData)
        throws IOException, StoreException
    {
        // Hard deletion is not hard at all (pun attack!)...
        if (removeExternalData && entry.hasExternalData()) {
            _deleteBackingFile(key, entry.getExternalFile(_fileManager));
        }
        _backend.deleteEntry(key);
        return entry;
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
            
    protected int _calcChecksum(ByteContainer data)
    {
        return data.hash(BlockMurmur3Hasher.instance, HASH_SEED);
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
            ByteContainer data)
    {
        return _compressionEnabled
            && (metadata.compression == null)
            && (data.byteLength() >= _minCompressibleSize)
            && !Compressors.isCompressed(data);
    }
    
    protected void _checkClosed()
    {
        if (_closed.get()) {
            throw new IllegalStateException("Can not access data from StorableStore after it has been closed");
        }
    }
}
