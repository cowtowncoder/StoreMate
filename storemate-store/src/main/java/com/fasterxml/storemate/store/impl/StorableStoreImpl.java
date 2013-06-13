package com.fasterxml.storemate.store.impl;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.BlockHasher32;
import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;
import com.fasterxml.storemate.shared.hash.HashConstants;
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
import com.fasterxml.storemate.store.util.OverwriteChecker;
import com.fasterxml.storemate.store.util.PartitionedWriteMutex;

/**
 * Full store front-end implementation.
 */
public class StorableStoreImpl extends AdminStorableStore
{
    /**
     * No real seed used for Murmur3/32.
     */
    private final static int HASH_SEED = BlockHasher32.DEFAULT_SEED;

    private final static OverwriteChecker OVERWRITE_OK = OverwriteChecker.AlwaysOkToOverwrite.instance;

    private final static OverwriteChecker OVERWRITE_NOT_OK = OverwriteChecker.NeverOkToOverwrite.instance;
    
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
     * We must ensure that read-modify-write operations are atomic;
     * this object is used for that.
     */
    protected final PartitionedWriteMutex _writeMutex;
    
    /**
     * We may also need to do primitive locking for read-modify-write
     * operations (which soft-delete is, for example); and/or throttle
     * number of concurrent operations of certain types.
     * Both can be implemented using chained set of throttlers.
     */
    protected final StoreOperationThrottler _throttler;
    
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

    @Deprecated
    public StorableStoreImpl(StoreConfig config, StoreBackend physicalStore,
            TimeMaster timeMaster, FileManager fileManager)
    {
        this(config, physicalStore, timeMaster, fileManager, null, null);
    }

    public StorableStoreImpl(StoreConfig config, StoreBackend physicalStore,
            TimeMaster timeMaster, FileManager fileManager,
            StoreOperationThrottler throttler, PartitionedWriteMutex writeMutex)
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

        if (throttler == null) {
            throttler = new StoreOperationThrottler.Base();
        }
        _throttler = throttler;
        if (writeMutex == null) {
            writeMutex = buildDefaultWriteMutex(config);
        }
        _writeMutex = writeMutex;
    }

    protected PartitionedWriteMutex buildDefaultWriteMutex(StoreConfig config)
    {
        // May want to make this configurable in future...
        // 'true' means "fair", minor overhead, prevents potential starvation
        /* 02-Jun-2013, tatu: Unless we have true concurrency, may NOT want
         *   fairness as it is not needed (if we serialize calls anyway) but
         *   still incurs overhead.
         */
        return new PartitionedWriteMutex(config.lockPartitions, true);
    }

    @Override
    public void start() throws Exception {
        _backend.start();
    }

    @Override
    public void prepareForStop() throws Exception {
        _backend.prepareForStop();
    }
    
    @Override
    public void stop() throws Exception
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

    @Override
    public StoreBackend getBackend() {
        return _backend;
    }

    @Override
    public StoreOperationThrottler getThrottler() {
        return _throttler;
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
        return _writeMutex.getOldestInFlightTimestamp();
    }

    /*
    /**********************************************************************
    /* API, data reads
    /**********************************************************************
     */

    @Override
    public boolean hasEntry(StoreOperationSource source, StorableKey key) throws StoreException
    {
        _checkClosed();
        return _backend.hasEntry(key);
    }

    @Override
    public Storable findEntry(StoreOperationSource source, StorableKey key) throws StoreException
    {
        _checkClosed();
        final long operationTime = _timeMaster.currentTimeMillis();
        try {
            return _throttler.performGet(source, operationTime, key, new StoreOperationCallback<Storable>() {
                @Override
                public Storable perform(long operationTime, StorableKey key, Storable value)
                        throws IOException, StoreException {
                    return _backend.findEntry(key);
                }
            });
        } catch (IOException e) {
            throw new StoreException.IO(key,
                    "Problem when trying to access entry: "+e.getMessage(), e);
        }
    }
    
    /*
    /**********************************************************************
    /* API, entry creation
    /**********************************************************************
     */
    
    @Override
    public StorableCreationResult insert(StoreOperationSource source, StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata)
        throws IOException, StoreException
    {
        _checkClosed();
        return _putEntry(source, key, input, stdMetadata, customMetadata, OVERWRITE_NOT_OK);
    }

    @Override
    public StorableCreationResult insert(StoreOperationSource source, StorableKey key, ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata)
        throws IOException, StoreException
    {
        _checkClosed();
        return _putEntry(source, key, input, stdMetadata, customMetadata, OVERWRITE_NOT_OK);
    }
    
    @Override
    public StorableCreationResult upsert(StoreOperationSource source,StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile)
        throws IOException, StoreException
    {
        _checkClosed();
        StorableCreationResult result = _putEntry(source, key, input, stdMetadata, customMetadata, OVERWRITE_OK);
        if (removeOldDataFile) {
            Storable old = result.getPreviousEntry();
            if (old != null) {
                _deleteBackingFile(key, old.getExternalFile(_fileManager));
            }
        }
        return result;
    }

    @Override
    public StorableCreationResult upsert(StoreOperationSource source, StorableKey key, ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile)
        throws IOException, StoreException
    {
        _checkClosed();
        StorableCreationResult result = _putEntry(source, key, input, stdMetadata, customMetadata, OVERWRITE_OK);
        if (removeOldDataFile) {
            Storable old = result.getPreviousEntry();
            if (old != null) {
                _deleteBackingFile(key, old.getExternalFile(_fileManager));
            }
        }
        return result;
    }

    @Override
    public StorableCreationResult upsertConditionally(StoreOperationSource source, StorableKey key,
            InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile, OverwriteChecker checker)
        throws IOException, StoreException
    {
        _checkClosed();
        StorableCreationResult result = _putEntry(source, key, input, stdMetadata, customMetadata, checker);
        if (removeOldDataFile) {
            Storable old = result.getPreviousEntry();
            if (old != null) {
                _deleteBackingFile(key, old.getExternalFile(_fileManager));
            }
        }
        return result;
    }

    @Override
    public StorableCreationResult upsertConditionally(StoreOperationSource source, StorableKey key,
            ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile, OverwriteChecker checker)
        throws IOException, StoreException
    {
        _checkClosed();
        StorableCreationResult result = _putEntry(source, key, input, stdMetadata, customMetadata, checker);
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
    protected StorableCreationResult _putEntry(StoreOperationSource source, StorableKey key,
            InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            OverwriteChecker allowOverwrites)
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
                    return _compressAndPutSmallEntry(source, key, stdMetadata, customMetadata,
                            allowOverwrites, ByteContainer.simple(readBuffer, 0, len));
                }
                return _putSmallPreCompressedEntry(source, key, stdMetadata, customMetadata,
                        allowOverwrites, ByteContainer.simple(readBuffer, 0, len));
            }
            // partial read in buffer, rest from input stream:
            return _putLargeEntry(source, key, stdMetadata, customMetadata,
                    allowOverwrites, readBuffer, len, input);
        } finally {
            bufferHolder.returnBuffer(readBuffer);
        }
    }

    protected StorableCreationResult _putEntry(StoreOperationSource source, StorableKey key,
            ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            OverwriteChecker allowOverwrites)
        throws IOException, StoreException
    {
        // First things first: verify that compression is what it claims to be:
        final Compression originalCompression = stdMetadata.compression;
        String error = IOUtil.verifyCompression(originalCompression, input);
        if (error != null) {
            throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM, error);
        }
        if (originalCompression == null) { // client did not compress, we may try to
            return _compressAndPutSmallEntry(source, key, stdMetadata, customMetadata,
                    allowOverwrites, input);
        }
        return _putSmallPreCompressedEntry(source, key, stdMetadata, customMetadata,
                allowOverwrites, input);
    }
    
    /*
    /**********************************************************************
    /* Internal methods for entry creation, second level
    /**********************************************************************
     */
    
    protected StorableCreationResult _compressAndPutSmallEntry(StoreOperationSource source, StorableKey key,
            StorableCreationMetadata metadata, ByteContainer customMetadata,
            OverwriteChecker allowOverwrites, ByteContainer data)
        throws IOException, StoreException
    {
        final int origLength = data.byteLength();
        // must verify checksum unless we got compressed payload
        // do we insist on checksum? Not if client has not yet compressed it:
        int actualChecksum = _calcChecksum(data);
        final int origChecksum = metadata.contentHash;
        if (origChecksum == HashConstants.NO_CHECKSUM) {
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
        return _putSmallEntry(source, key, metadata, customMetadata, allowOverwrites, data);
    }

    protected StorableCreationResult _putSmallPreCompressedEntry(StoreOperationSource source, StorableKey key,
            StorableCreationMetadata metadata, ByteContainer customMetadata,
            OverwriteChecker allowOverwrites, ByteContainer data)
        throws IOException, StoreException
    {
        /* !!! TODO: what to do with checksum? Should we require checksum
         *   of raw or compressed entity? (or both); whether to store both;
         *   verify etc...
         */
        final int origChecksum = metadata.contentHash;
        if (origChecksum == HashConstants.NO_CHECKSUM) {
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
            if (metadata.compressedContentHash == HashConstants.NO_CHECKSUM) {
                metadata.compressedContentHash = _calcChecksum(data);
            }
        }
        return _putSmallEntry(source, key, metadata, customMetadata, allowOverwrites, data);
    }

    protected StorableCreationResult _putSmallEntry(StoreOperationSource source, final StorableKey key,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            OverwriteChecker allowOverwrites, final ByteContainer data)
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
            final long fileCreationTime = _timeMaster.currentTimeMillis();
            FileReference fileRef = _fileManager.createStorageFile(key,
                    stdMetadata.compression, fileCreationTime);
            try {
                _throttler.performFileWrite(new FileOperationCallback<Void>() {
                    @Override
                    public Void perform(long operationTime, StorableKey key, Storable value, File externalFile)
                            throws IOException, StoreException {
                        IOUtil.writeFile(externalFile, data);
                        return null;
                    }
                }, fileCreationTime, key, fileRef.getFile());
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
        return _putPartitionedEntry(source, key, creationTime, stdMetadata, storable, allowOverwrites);
    }

    @SuppressWarnings("resource")
    protected StorableCreationResult _putLargeEntry(StoreOperationSource source, final StorableKey key,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            OverwriteChecker allowOverwrites,
            final byte[] readBuffer, final int readByteCount,
            final InputStream input)
        throws IOException, StoreException
    {
        final boolean skipCompression;
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
        
        final OutputStream out;
        final CountingOutputStream compressedOut;

        if (skipCompression) {
            compressedOut = null;
            out = new FileOutputStream(storedFile);
        } else {
            compressedOut = new CountingOutputStream(new FileOutputStream(storedFile),
                    new IncrementalMurmur3Hasher());
            out = Compressors.compressingStream(compressedOut, comp);
        }
        final IncrementalMurmur3Hasher hasher = new IncrementalMurmur3Hasher(HASH_SEED);        
        
        /* 04-Jun-2013, tatu: Rather long block of possibly throttled file-writing
         *    action... will need to be straightened out in due time.
         */
        long copiedBytes = _throttler.performFileWrite(new FileOperationCallback<Long>() {
            @Override
            public Long perform(long operationTime, StorableKey key, Storable value, File externalFile)
                    throws IOException, StoreException {
                try {
                    out.write(readBuffer, 0, readByteCount);
                } catch (IOException e) {
                    try {
                        if (out != null) {
                            out.close();
                        }
                    } catch (IOException e2) { }
                    throw new StoreException.IO(key, "Failed to write initial "+readByteCount+" bytes of file '"+externalFile.getAbsolutePath()+"'", e);
                }
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
                                    +") to file '"+externalFile.getAbsolutePath()+"'", e);
                        }
                        hasher.update(readBuffer, 0, count);
                    }
                } finally {
                    try {
                        out.close();
                    } catch (IOException e) { }
                }
                return copiedBytes;
            }
        }, fileCreationTime, key, fileRef.getFile());
        
        // Checksum calculation and storage details differ depending on whether compression is used
        if (skipCompression) {
            // Storage sizes must match, first of all, if provided
            if (stdMetadata.storageSize != copiedBytes && stdMetadata.storageSize >= 0) {
                throw new StoreException.Input(key, StoreException.InputProblem.BAD_LENGTH,
                        "Incorrect length for entry; storageSize="+stdMetadata.storageSize
                        +", bytes read: "+copiedBytes);
            }

            final int actualHash = _cleanChecksum(hasher.calculateHash());
            stdMetadata.storageSize = copiedBytes;
            if (stdMetadata.compression == Compression.NONE) {
                if (stdMetadata.contentHash == HashConstants.NO_CHECKSUM) {
                    stdMetadata.contentHash = actualHash;
                } else if (stdMetadata.contentHash != actualHash) {
                    throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM,
                            "Incorrect checksum for not-compressed entry ("+copiedBytes+" bytes): got 0x"
                                    +Integer.toHexString(stdMetadata.contentHash)+", calculated to be 0x"
                                    +Integer.toHexString(actualHash));
                }
            } else { // already compressed
//                stdMetadata.compressedContentHash = _cleanChecksum(hasher.calculateHash());
                if (stdMetadata.compressedContentHash == HashConstants.NO_CHECKSUM) {
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
            final int contentHash = _cleanChecksum(hasher.calculateHash());
            final int compressedHash = _cleanChecksum(compressedOut.calculateHash());
            
            stdMetadata.uncompressedSize = copiedBytes;
            stdMetadata.storageSize = compressedOut.count();
            // must verify checksum, if one was offered...
            if (stdMetadata.contentHash == HashConstants.NO_CHECKSUM) {
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
            if (stdMetadata.compressedContentHash == HashConstants.NO_CHECKSUM) {
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

        return _putPartitionedEntry(source, key, creationTime, stdMetadata, storable, allowOverwrites);
    }

    /**
     * Method called to actually write the entry metadata in local database.
     * 
     * @param operationTime Timestamp used as the "last-modified" timestamp in metadata;
     *   important as it determines last-modified traversal order for synchronization
     */
    protected StorableCreationResult _putPartitionedEntry(StoreOperationSource source, StorableKey key,
            final long operationTime,
            final StorableCreationMetadata stdMetadata, Storable storable,
            final OverwriteChecker allowOverwrites)
        throws IOException, StoreException
    {
        final StorableCreationResult result = _throttler.performPut(new StoreOperationCallback<StorableCreationResult>() {
            @Override
            public StorableCreationResult perform(long time, StorableKey key, final Storable newValue)
                throws IOException, StoreException
            {
                // blind update, insert-only are easy
                Boolean defaultOk = allowOverwrites.mayOverwrite(key);
                if (defaultOk != null) { // depends on entry in question...
                    if (defaultOk.booleanValue()) { // always ok, fine ("upsert")
                        return _writeMutex.partitionedWrite(new PartitionedWriteMutex.Callback<StorableCreationResult>() {
                            @Override
                            public StorableCreationResult performWrite(StorableKey key) throws IOException, StoreException {
                                Storable oldValue =  _backend.putEntry(key, newValue);
                                return new StorableCreationResult(key, true, newValue, oldValue);
                            }
                        }, time, key);
                    }
                    // strict "insert"
                    return _writeMutex.partitionedWrite(new PartitionedWriteMutex.Callback<StorableCreationResult>() {
                        @Override
                        public StorableCreationResult performWrite(StorableKey key) throws IOException, StoreException {
                            Storable oldValue =  _backend.createEntry(key, newValue);
                            if (oldValue == null) { // ok, succeeded
                                return new StorableCreationResult(key, true, newValue, null);
                            }
                            // fail: caller may need to clean up the underlying file
                            return new StorableCreationResult(key, false, newValue, oldValue);
                        }
                    }, time, key);
                }
                // But if things depend on existence of old entry, or entries, trickier:
                return _writeMutex.partitionedWrite(new PartitionedWriteMutex.Callback<StorableCreationResult>() {
                    @Override
                    public StorableCreationResult performWrite(StorableKey key) throws IOException, StoreException {
                        AtomicReference<Storable> oldEntryRef = new AtomicReference<Storable>();                       
                        if (!_backend.upsertEntry(key, newValue, allowOverwrites, oldEntryRef)) {
                            // fail due to existing entry
                            return new StorableCreationResult(key, false, newValue, oldEntryRef.get());
                        }
                        return new StorableCreationResult(key, true, newValue, oldEntryRef.get());
                    }
                }, time, key);
            }
        },
        operationTime, key, storable);

        //_partitions.put(key, stdMetadata, storable, allowOverwrite);
        if (!result.succeeded()) {
            // One piece of clean up: for failed insert, delete backing file, if any
//            if (!allowOverwrite) {
            // otherwise, may need to delete file that was created
            FileReference ref = stdMetadata.dataFile;
            if (ref != null) {
                _deleteBackingFile(key, ref.getFile());
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
    public StorableDeletionResult softDelete(final StoreOperationSource source, StorableKey key,
            final boolean removeInlinedData, final boolean removeExternalData)
        throws IOException, StoreException
    {
        _checkClosed();
        Storable entry = _throttler.performSoftDelete(new StoreOperationCallback<Storable>() {
            @Override
            public Storable perform(final long operationTime, StorableKey key, Storable value)
                throws IOException, StoreException
            {
                return _writeMutex.partitionedWrite(new PartitionedWriteMutex.Callback<Storable>() {
                    @Override
                    public Storable performWrite(StorableKey key) throws IOException, StoreException {
                        Storable value = _backend.findEntry(key);              
                        // First things first: if no entry, nothing to do
                        if (value == null) {
                            return null;
                        }
                        return _softDelete(source, key, value, operationTime, removeInlinedData, removeExternalData);
                    }
                }, operationTime, key);
            }
        }, _timeMaster.currentTimeMillis(), key);
        return new StorableDeletionResult(key, entry);
    }
    
    @Override
    public StorableDeletionResult hardDelete(final StoreOperationSource source, StorableKey key,
            final boolean removeExternalData)
        throws IOException, StoreException
    {
        _checkClosed();
        Storable entry = _throttler.performHardDelete(new StoreOperationCallback<Storable>() {
            @Override
            public Storable perform(final long operationTime, StorableKey key, Storable value)
                throws IOException, StoreException
            {
                return _writeMutex.partitionedWrite(new PartitionedWriteMutex.Callback<Storable>() {
                    @Override
                    public Storable performWrite(StorableKey key) throws IOException, StoreException {
                        Storable value = _backend.findEntry(key);              
                        // First things first: if no entry, nothing to do
                        if (value == null) {
                            return null;
                        }
                        return _hardDelete(source, key, value, removeExternalData);
                    }
                }, operationTime, key);
            }
        }, _timeMaster.currentTimeMillis(), key);
        return new StorableDeletionResult(key, entry);
    }

    protected Storable _softDelete(StoreOperationSource source, StorableKey key,
            Storable entry, final long currentTime,
            final boolean removeInlinedData, final boolean removeExternalData)
        throws IOException, StoreException
    {
        // Ok now... need to delete some data?
        boolean hasExternalToDelete = removeExternalData && entry.hasExternalData();
        if (!entry.isDeleted() || hasExternalToDelete
                || (removeInlinedData && entry.hasInlineData())) {
            File extFile = hasExternalToDelete ? entry.getExternalFile(_fileManager) : null;
            entry = _storableConverter.softDeletedCopy(key, entry, currentTime,
                    removeInlinedData, removeExternalData);
            _backend.ovewriteEntry(key, entry);
            if (extFile != null) {
                _deleteBackingFile(key, extFile);
            }
        }
        return entry;
    }

    protected Storable _hardDelete(StoreOperationSource source,
            StorableKey key, Storable entry,
            final boolean removeExternalData)
        throws IOException, StoreException
    {
        _backend.deleteEntry(key);
        // Hard deletion is not hard at all (pun attack!)...
        if (removeExternalData && entry.hasExternalData()) {
            _deleteBackingFile(key, entry.getExternalFile(_fileManager));
        }
        return entry;
    }    
 
    /*
    /**********************************************************************
    /* API, public entry iteration methods
    /**********************************************************************
     */
    
    @Override
    public IterationResult iterateEntriesByKey(StoreOperationSource source,
            final StorableKey firstKey,
            final StorableIterationCallback cb)
        throws StoreException
    {
        try {
            return _throttler.performList(new StoreOperationCallback<IterationResult>() {
                @Override
                public IterationResult perform(long operationTime, StorableKey key, Storable value)
                        throws IOException, StoreException {
                    return _backend.iterateEntriesByKey(cb, firstKey);
                }
            }, _timeMaster.currentTimeMillis());
        } catch (IOException e) {
            throw new StoreException.IO(firstKey, "Failed to iterate entries from "+firstKey+": "+e.getMessage(), e);
        }
    }

    @Override
    public IterationResult iterateEntriesAfterKey(StoreOperationSource source,
            final StorableKey lastSeen,
            final StorableIterationCallback cb)
        throws StoreException
    {
        try {
            return _throttler.performList(new StoreOperationCallback<IterationResult>() {
                @Override
                public IterationResult perform(long operationTime, StorableKey key, Storable value)
                        throws IOException, StoreException {
                    // if we didn't get "lastSeen", same as regular method
                    if (lastSeen == null) {
                        return _backend.iterateEntriesByKey(cb, null);
                    }
                    return _backend.iterateEntriesAfterKey(cb, lastSeen);
                }
            }, _timeMaster.currentTimeMillis());
        } catch (IOException e) {
            throw new StoreException.IO(lastSeen, "Failed to iterate entries from "+lastSeen+": "+e.getMessage(), e);
        }
    }
    
    @Override
    public IterationResult iterateEntriesByModifiedTime(StoreOperationSource source,
            long firstTimestamp,
            StorableLastModIterationCallback cb)
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
    public int getInFlightWritesCount() {
        return _writeMutex.getInFlightWritesCount();
    }
    
    @Override
    public long getTombstoneCount(StoreOperationSource source, long maxRuntimeMsecs)
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
    public List<Storable> dumpEntries(StoreOperationSource source,
            final int maxCount, final boolean includeDeleted)
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
    public List<Storable> dumpOldestEntries(StoreOperationSource source, final int maxCount,
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
    public int removeEntries(StoreOperationSource source, final int maxToRemove)
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
                hardDelete(source, key, true);
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
    public int removeTombstones(StoreOperationSource source, final int maxToRemove)
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
                hardDelete(source, key, true);
                ++removed;
            }
        }
        return removed;
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
            
    protected static int _calcChecksum(ByteContainer data) {
        // important: mask zero value, which occurs with empty content
        return _cleanChecksum(data.hash(BlockMurmur3Hasher.instance, HASH_SEED));
    }

    protected static int _cleanChecksum(int checksum) {
        return (checksum == HashConstants.NO_CHECKSUM) ? HashConstants.CHECKSUM_FOR_ZERO : checksum;
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
