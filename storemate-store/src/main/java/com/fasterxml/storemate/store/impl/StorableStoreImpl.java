package com.fasterxml.storemate.store.impl;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.*;
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
import com.fasterxml.storemate.store.util.*;
import com.fasterxml.util.membuf.MemBuffersForBytes;
import com.fasterxml.util.membuf.StreamyBytesMemBuffer;

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
     * By default we'll use off-heap buffers composed of 64kB segments.
     */
    protected final int OFF_HEAP_BUFFER_SEGMENT_LEN = 64000;
    
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

    /*
    /**********************************************************************
    /* Helper objects for buffering
    /**********************************************************************
     */
    
    /**
     * We can reuse read buffers as they are somewhat costly to
     * allocate, reallocate all the time. Buffer used needs to be big
     * enough to contain all conceivably inlineable cases (considering
     * possible compression).
     * Currently we'll use 64k as the cut-off point.
     */
    protected final static BufferRecycler _readBuffers = new BufferRecycler(StoreConfig.DEFAULT_MIN_PAYLOAD_FOR_STREAMING);

    /**
     * Beyond simple read/write buffer, let's also use bigger off-heap buffers for
     * larger entries.
     */
    protected final MemBuffersForBytes _offHeapBuffers;

    /**
     * Plus we also need to know configuration for buffers to construct.
     */
    protected final int _maxSegmentsPerBuffer;
    
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

        /* And then sizing for off-heap buffers... granularity of
         * 64kB per buffer seems reasonable, and we can derive other
         * attributes from that.
         */
        long totalSize = config.offHeapBufferSize.getNumberOfBytes();
        int totalSegments = (int)(totalSize + OFF_HEAP_BUFFER_SEGMENT_LEN - 1) / OFF_HEAP_BUFFER_SEGMENT_LEN;
        // let's prevent ridiculously small buffers tho:
        if (totalSegments < 10) {
            totalSegments = 10;
        }
        int maxPerBuffer = (int) (config.maxPerEntryBuffering.getNumberOfBytes() + OFF_HEAP_BUFFER_SEGMENT_LEN - 1) / OFF_HEAP_BUFFER_SEGMENT_LEN;
        _maxSegmentsPerBuffer = Math.max(2,  maxPerBuffer);
        // and pre-allocate quarter of those buffers right away?
        _offHeapBuffers = new MemBuffersForBytes(OFF_HEAP_BUFFER_SEGMENT_LEN, totalSegments/4, totalSegments);
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

    @Override
    public <T> T leaseOffHeapBuffer(ByteBufferCallback<T> cb)
    {
        StreamyBytesMemBuffer buffer = null;
        try {
            buffer = allocOffHeapBuffer();
            return cb.withBuffer(buffer);
        } catch (IllegalStateException e) {
            return cb.withError(e);
        } finally {
            if (buffer != null) {
                buffer.close();
            }
        }
    }

    /**
     * Internal method that tries to allocate an off-heap buffer.
     */
    protected StreamyBytesMemBuffer allocOffHeapBuffer() {
        return _offHeapBuffers.createStreamyBuffer(2, _maxSegmentsPerBuffer);
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
    public boolean hasEntry(final StoreOperationSource source, final OperationDiagnostics diag,
            StorableKey key0)
        throws StoreException
    {
        _checkClosed();
        long operationTime0 = _timeMaster.currentTimeMillis();
        final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
        try {
            return _throttler.performHas(source, operationTime0, key0, new StoreOperationCallback<Boolean>() {
                @Override
                public Boolean perform(long operationTime, StorableKey key, Storable value)
                        throws StoreException {
                    final long dbStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                    try {
                        return _backend.hasEntry(key);
                    } finally {
                        if (diag != null) {
                            diag.addDbAccess(nanoStart, dbStart, _timeMaster.nanosForDiagnostics());
                        }
                    }
                }
            });
        } catch (IOException e) {
            throw new StoreException.IO(key0,
                    "Problem when trying to access entry: "+e.getMessage(), e);
        }
    }
    
    @Override
    public Storable findEntry(final StoreOperationSource source,
            final OperationDiagnostics diag,
            StorableKey key0) throws StoreException
    {
        _checkClosed();
        long operationTime0 = _timeMaster.currentTimeMillis();
        final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
        try {
            return _throttler.performGet(source, operationTime0, key0, new StoreOperationCallback<Storable>() {
                @Override
                public Storable perform(long operationTime, StorableKey key, Storable value)
                        throws IOException, StoreException {
                    final long dbStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                    Storable result = _backend.findEntry(key);
                    if (diag != null) {
                        diag.addDbAccess(nanoStart, dbStart, _timeMaster.nanosForDiagnostics());
                        diag.setEntry(result);
                    }
                    return result;
                }
            });
        } catch (IOException e) {
            throw new StoreException.IO(key0,
                    "Problem when trying to access entry: "+e.getMessage(), e);
        }
    }
    
    /*
    /**********************************************************************
    /* API, entry creation
    /**********************************************************************
     */
    
    @Override
    public StorableCreationResult insert(StoreOperationSource source, OperationDiagnostics diag,
            StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata)
        throws IOException, StoreException
    {
        _checkClosed();
        return _putEntry(source, diag, key, input, stdMetadata, customMetadata, OVERWRITE_NOT_OK);
    }

    @Override
    public StorableCreationResult insert(StoreOperationSource source, OperationDiagnostics diag,
            StorableKey key, ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata)
        throws IOException, StoreException
    {
        _checkClosed();
        return _putEntry(source, diag, key, input, stdMetadata, customMetadata, OVERWRITE_NOT_OK);
    }
    
    @Override
    public StorableCreationResult upsert(StoreOperationSource source, OperationDiagnostics diag,
            StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile)
        throws IOException, StoreException
    {
        _checkClosed();
        StorableCreationResult result = _putEntry(source, diag, key, input, stdMetadata, customMetadata, OVERWRITE_OK);
        if (removeOldDataFile) {
            Storable old = result.getPreviousEntry();
            if (old != null) {
                _deleteBackingFile(key, old.getExternalFile(_fileManager));
            }
        }
        return result;
    }

    @Override
    public StorableCreationResult upsert(StoreOperationSource source, OperationDiagnostics diag,
            StorableKey key, ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile)
        throws IOException, StoreException
    {
        _checkClosed();
        StorableCreationResult result = _putEntry(source, diag, key, input, stdMetadata, customMetadata, OVERWRITE_OK);
        if (removeOldDataFile) {
            Storable old = result.getPreviousEntry();
            if (old != null) {
                _deleteBackingFile(key, old.getExternalFile(_fileManager));
            }
        }
        return result;
    }

    @Override
    public StorableCreationResult upsertConditionally(StoreOperationSource source, OperationDiagnostics diag,
            StorableKey key, InputStream input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile, OverwriteChecker checker)
        throws IOException, StoreException
    {
        _checkClosed();
        StorableCreationResult result = _putEntry(source, diag, key, input, stdMetadata, customMetadata, checker);
        if (removeOldDataFile) {
            Storable old = result.getPreviousEntry();
            if (old != null) {
                _deleteBackingFile(key, old.getExternalFile(_fileManager));
            }
        }
        return result;
    }

    @Override
    public StorableCreationResult upsertConditionally(StoreOperationSource source, OperationDiagnostics diag,
            StorableKey key, ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            boolean removeOldDataFile, OverwriteChecker checker)
        throws IOException, StoreException
    {
        _checkClosed();
        StorableCreationResult result = _putEntry(source, diag, key, input, stdMetadata, customMetadata, checker);
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
    protected StorableCreationResult _putEntry(StoreOperationSource source, OperationDiagnostics diag,
            StorableKey key, InputStream input,
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
            // !!! TODO: only partial read... should include other parts too
            final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
            try {
                len = IOUtil.readFully(input, readBuffer);
            } catch (IOException e) {
                throw new StoreException.IO(key, "Failed to read payload for key "+key+": "+e.getMessage(), e);
            }
            if (diag != null) {
                diag.addRequestReadTime(nanoStart, _timeMaster);
            }
    
            // First things first: verify that compression is what it claims to be:
            final Compression originalCompression = stdMetadata.compression;
            String error = IOUtil.verifyCompression(originalCompression, readBuffer, len);
            if (error != null) {
                throw new StoreException.Input(key, StoreException.InputProblem.BAD_COMPRESSION, error);
            }
            if (len < readBuffer.length) { // read it all: we are done with input stream
                if (originalCompression == null) { // client did not compress, we may try to
                    return _compressAndPutSmallEntry(source, diag, key, stdMetadata, customMetadata,
                            allowOverwrites, ByteContainer.simple(readBuffer, 0, len));
                }
                return _putSmallPreCompressedEntry(source, diag, key, stdMetadata, customMetadata,
                        allowOverwrites, ByteContainer.simple(readBuffer, 0, len));
            }
            // partial read in buffer, rest from input stream:
            return _putLargeEntry(source, diag, key, stdMetadata, customMetadata,
                    allowOverwrites, readBuffer, len, input);
        } finally {
            bufferHolder.returnBuffer(readBuffer);
        }
    }

    protected StorableCreationResult _putEntry(StoreOperationSource source, OperationDiagnostics diag,
            StorableKey key, ByteContainer input,
            StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            OverwriteChecker allowOverwrites)
        throws IOException, StoreException
    {
        // First things first: verify that compression is what it claims to be:
        final Compression origComp = stdMetadata.compression;
        String error = IOUtil.verifyCompression(origComp, input);
        if (error != null) {
            throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM, error);
        }

        if (origComp == null) { // client did not compress, we may try to
            return _compressAndPutSmallEntry(source, diag, key, stdMetadata, customMetadata,
                    allowOverwrites, input);
        }
        return _putSmallPreCompressedEntry(source, diag, key, stdMetadata, customMetadata,
                allowOverwrites, input);
    }
    
    /*
    /**********************************************************************
    /* Internal methods for entry creation, second level
    /**********************************************************************
     */
    
    protected StorableCreationResult _compressAndPutSmallEntry(StoreOperationSource source, OperationDiagnostics diag,
            StorableKey key, StorableCreationMetadata metadata, ByteContainer customMetadata,
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
        return _putSmallEntry(source, diag, key, metadata, customMetadata, allowOverwrites, data);
    }

    protected StorableCreationResult _putSmallPreCompressedEntry(StoreOperationSource source,
            OperationDiagnostics diag,
            StorableKey key, StorableCreationMetadata metadata, ByteContainer customMetadata,
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
        final Compression comp = metadata.compression;
        if (comp != Compression.NONE) {
            if (metadata.compressedContentHash == HashConstants.NO_CHECKSUM) {
                metadata.compressedContentHash = _calcChecksum(data);
            }
            final long origLength = metadata.uncompressedSize;
            if (origLength <= 0L) {
                throw new StoreException.Input(key, StoreException.InputProblem.BAD_LENGTH,
                        "Missing or invalid uncompressedSize ("+origLength+") for pre-compressed ("
                        +metadata.compression+") content");
            }
        }
        metadata.storageSize = data.byteLength();
        return _putSmallEntry(source, diag, key, metadata, customMetadata, allowOverwrites, data);
    }

    protected StorableCreationResult _putSmallEntry(final StoreOperationSource source, final OperationDiagnostics diag,
            final StorableKey key,
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
                final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                _throttler.performFileWrite(source, fileCreationTime, key, fileRef.getFile(),
                        new FileOperationCallback<Void>() {
                    @Override
                    public Void perform(long operationTime, StorableKey key, Storable value, File externalFile)
                            throws IOException, StoreException {
                        final long fsStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                        IOUtil.writeFile(externalFile, data);
                        if (diag != null) {
                            diag.addFileWriteAccess(nanoStart,  fsStart,  _timeMaster, data.byteLength());
                        }
                        return null;
                    }
                });
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
        return _putPartitionedEntry(source, diag, key, creationTime, stdMetadata, storable, allowOverwrites);
    }

    protected StorableCreationResult _putLargeEntry(StoreOperationSource source, final OperationDiagnostics diag,
            final StorableKey key, StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            OverwriteChecker allowOverwrites,
            final byte[] readBuffer, int readByteCount,
            final InputStream input)
        throws IOException, StoreException
    {
        final boolean skipCompression;
        
        if (stdMetadata.compression != null) { // pre-compressed, or blocked (explicit "none")
            skipCompression = true;
        } else {
            if (!_compressionEnabled || Compressors.isCompressed(readBuffer, 0, readByteCount)) {
                skipCompression = true;
                stdMetadata.compression = Compression.NONE;
            } else {
                skipCompression = false;
                stdMetadata.compression = Compression.LZF;
            }
        }
        
        // First things first: safe handling of off-heap buffer...
        StreamyBytesMemBuffer offHeap = allocOffHeapBuffer();
        try {
            return _putLargeEntry2(source, diag,
                    key, stdMetadata, customMetadata,
                    allowOverwrites, readBuffer, readByteCount, input,
                    skipCompression,
                    offHeap);
        } finally {
            if (offHeap != null) {
                offHeap.close();
            }
        }
    }

    @SuppressWarnings("resource")
    protected StorableCreationResult _putLargeEntry2(StoreOperationSource source, final OperationDiagnostics diag,
            final StorableKey key, StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            OverwriteChecker allowOverwrites,
            final byte[] readBuffer, int incomingReadByteCount,
            final InputStream input,
            final boolean skipCompression,
            final StreamyBytesMemBuffer offHeap)
        throws IOException, StoreException
    {
        /* First: let's see if we can't just read and buffer all the content in
         * an off-heap buffer (if we got one).
         */
        final byte[] leftover;
        if (offHeap == null) { // unlikely to ever occur, but theoretically possible so:
            leftover = Arrays.copyOf(readBuffer, incomingReadByteCount);
        } else {
            if (offHeap != null) {
                if (!offHeap.tryAppend(readBuffer, 0, incomingReadByteCount)) {
                    throw new IOException("Internal problem: failed to append "+incomingReadByteCount+" in an off-heap buffer");
                }
            }
            int overflow = _readInBuffer(diag, key, input, readBuffer, offHeap);
            if (overflow == 0) {
                // Optimal case: managed to read all input -- offline!
                return _putLargeEntryFullyBuffered(source, diag,
                        key, stdMetadata, customMetadata, allowOverwrites,
                        readBuffer, skipCompression, offHeap);
            }
            leftover = Arrays.copyOf(readBuffer, overflow);
        }

        /* No go: could not buffer all content. If so, copy whatever we have in a "left-over"
         * bucket, start cranking...
         */
        
        // So: start by creating the result file
        long fileCreationTime = _timeMaster.currentTimeMillis();
        final FileReference fileRef = _fileManager.createStorageFile(key, stdMetadata.compression, fileCreationTime);
        File storedFile = fileRef.getFile();

        final OutputStream out;
        final CountingOutputStream compressedOut;

        if (skipCompression) {
            compressedOut = null;
            out = new FileOutputStream(storedFile);
        } else {
            compressedOut = new CountingOutputStream(new FileOutputStream(storedFile),
                    new IncrementalMurmur3Hasher());
            out = Compressors.compressingStream(compressedOut, stdMetadata.compression);
        }
        final IncrementalMurmur3Hasher hasher = new IncrementalMurmur3Hasher(HASH_SEED);        

        // Need to mix-n-match read, write; trickier to account for each part.
        final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
        long copiedBytes = _throttler.performFileWrite(source,
                fileCreationTime, key, fileRef.getFile(),
                new FileOperationCallback<Long>() {
            @Override
            public Long perform(long operationTime, StorableKey key, Storable value, File externalFile)
                    throws IOException, StoreException {
                final long fsStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                long copiedBytes = 0L;

                try {
                    // First: dump out anything in off-heap buffer
                    if (offHeap != null) {
                        int count;
                        while ((count = offHeap.readIfAvailable(readBuffer)) > 0) {
                            out.write(readBuffer, 0, count);
                            copiedBytes += count;
                            hasher.update(readBuffer, 0, count);
                        }
                    }
                    // then any leftovers
                    if (leftover != null) {
                        out.write(leftover);
                        hasher.update(leftover, 0, leftover.length);
                        copiedBytes += leftover.length;
                    }
                    // and then need to proceed with copying the rest, compressing along the way
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
                        out.write(readBuffer, 0, count);
                        hasher.update(readBuffer, 0, count);
                    }
                } catch (IOException e) {
                    if (copiedBytes == 0L) {
                        throw new StoreException.IO(key, "Failed to write initial bytes of file '"+externalFile.getAbsolutePath()+"'", e);
                    }
                    throw new StoreException.IO(key, "Failed to write intermediate bytes (after "+copiedBytes
                            +") to file '"+externalFile.getAbsolutePath()+"'", e);
                } finally {
                    try {
                        out.close();
                    } catch (IOException e) {
                        LOG.warn("Failed to close file {}: {}", externalFile, e.getMessage());
                    }
                    if (diag != null) {
                        // Note: due to compression, bytes written may be less than read:
                        long writtenBytes = (compressedOut == null) ? copiedBytes : compressedOut.count();
                        diag.addFileWriteAccess(nanoStart,  fsStart,  _timeMaster, writtenBytes);
                    }
                }
                return copiedBytes;
            }
        });
        // Checksum calculation and storage details differ depending on whether compression is used

        final int contentHash = _cleanChecksum(hasher.calculateHash());
        if (skipCompression) {
            _verifyStorageSize(key, stdMetadata, copiedBytes);
            if (stdMetadata.compression == Compression.NONE) {
                _verifyContentHash(key, stdMetadata, copiedBytes, contentHash);
            } else { // already compressed
                _verifyCompressedHash(key, stdMetadata, copiedBytes, contentHash);
            }
            // we don't really know the original size, either way:
            stdMetadata.uncompressedSize = 0L;
        } else {
            final int compressedHash = _cleanChecksum(compressedOut.calculateHash());
            stdMetadata.uncompressedSize = copiedBytes;
            stdMetadata.storageSize = compressedOut.count();
            // must verify checksum, if one was offered...
            _verifyContentHash(key, stdMetadata, copiedBytes, contentHash);
            _verifyCompressedHash(key, stdMetadata, copiedBytes, compressedHash);
        }
        long creationTime = _timeMaster.currentTimeMillis();
        Storable storable = _storableConverter.encodeOfflined(key, creationTime,
                stdMetadata, customMetadata, fileRef);

        return _putPartitionedEntry(source, diag, key, creationTime, stdMetadata, storable, allowOverwrites);
    }

    /**
     * Method used when the whole input did fit in off-heap buffer, and can be efficiently
     * written in file.
     */
    @SuppressWarnings("resource")
    protected StorableCreationResult _putLargeEntryFullyBuffered(StoreOperationSource source, final OperationDiagnostics diag,
            final StorableKey key, StorableCreationMetadata stdMetadata, ByteContainer customMetadata,
            OverwriteChecker allowOverwrites,
            final byte[] readBuffer, final boolean skipCompression, final StreamyBytesMemBuffer offHeap)
        throws IOException, StoreException
    {
        long fileCreationTime = _timeMaster.currentTimeMillis();
        final FileReference fileRef = _fileManager.createStorageFile(key, stdMetadata.compression, fileCreationTime);
        File storedFile = fileRef.getFile();

        final OutputStream out;
        final CountingOutputStream compressedOut;

        if (skipCompression) {
            compressedOut = null;
            out = new FileOutputStream(storedFile);
        } else {
            compressedOut = new CountingOutputStream(new FileOutputStream(storedFile),
                    new IncrementalMurmur3Hasher());
            out = Compressors.compressingStream(compressedOut, stdMetadata.compression);
        }
        final IncrementalMurmur3Hasher hasher = new IncrementalMurmur3Hasher(HASH_SEED);        

        final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
        long copiedBytes = _throttler.performFileWrite(source,
                fileCreationTime, key, fileRef.getFile(),
                new FileOperationCallback<Long>() {
            @Override
            public Long perform(long operationTime, StorableKey key, Storable value, File externalFile)
                    throws IOException, StoreException {
                final long fsStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                long copiedBytes = 0L;
                
                try {
                    int count;
                    while ((count = offHeap.readIfAvailable(readBuffer)) > 0) {
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
                    try { out.close(); } catch (IOException e) { }
                    if (diag != null) {
                        // Note: due to compression, bytes written may be less than read:
                        long writtenBytes = (compressedOut == null) ? copiedBytes : compressedOut.count();
                        diag.addFileWriteAccess(nanoStart,  fsStart,  _timeMaster, writtenBytes);
                    }
                }
                return copiedBytes;
            }
        });
        // Checksum calculation and storage details differ depending on whether compression is used
        final int contentHash = _cleanChecksum(hasher.calculateHash());
        if (skipCompression) {
            _verifyStorageSize(key, stdMetadata, copiedBytes);
            if (stdMetadata.compression == Compression.NONE) {
                _verifyContentHash(key, stdMetadata, copiedBytes, contentHash);
            } else { // already compressed
                _verifyCompressedHash(key, stdMetadata, copiedBytes, contentHash);
            }
            // we don't really know the original size, either way:
            stdMetadata.uncompressedSize = 0L;
        } else {
            final int compressedHash = _cleanChecksum(compressedOut.calculateHash());
            stdMetadata.uncompressedSize = copiedBytes;
            stdMetadata.storageSize = compressedOut.count();
            // must verify checksum, if one was offered...
            _verifyContentHash(key, stdMetadata, copiedBytes, contentHash);
            _verifyCompressedHash(key, stdMetadata, copiedBytes, compressedHash);
        }
        long creationTime = _timeMaster.currentTimeMillis();
        Storable storable = _storableConverter.encodeOfflined(key, creationTime,
                stdMetadata, customMetadata, fileRef);

        return _putPartitionedEntry(source, diag, key, creationTime, stdMetadata, storable, allowOverwrites);
    }
    
    protected void _verifyStorageSize(StorableKey key, StorableCreationMetadata stdMetadata, long bytes)
        throws StoreException
    {
        // Storage sizes must match, first of all, if provided
        if (stdMetadata.storageSize != bytes && stdMetadata.storageSize >= 0) {
            throw new StoreException.Input(key, StoreException.InputProblem.BAD_LENGTH,
                    "Incorrect length for entry; storageSize="+stdMetadata.storageSize
                    +", bytes read: "+bytes);
        }
        stdMetadata.storageSize = bytes;
    }
    
    protected void _verifyContentHash(StorableKey key, StorableCreationMetadata stdMetadata,
            long bytes, int contentHash)
        throws StoreException
    {
        if (stdMetadata.contentHash == HashConstants.NO_CHECKSUM) {
            stdMetadata.contentHash = contentHash;
        } else if (stdMetadata.contentHash != contentHash) {
            throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM,
                    "Incorrect content checksum for entry (compression: "+stdMetadata.compression
                    +", "+bytes+" bytes): got 0x"
                    +Integer.toHexString(stdMetadata.contentHash)+", calculated to be 0x"
                    +Integer.toHexString(contentHash));
        }
    }

    protected void _verifyCompressedHash(StorableKey key, StorableCreationMetadata stdMetadata,
            long bytes, int contentHash)
        throws StoreException
    {
        if (stdMetadata.compressedContentHash == HashConstants.NO_CHECKSUM) {
            stdMetadata.compressedContentHash = contentHash;
        } else {
            if (stdMetadata.compressedContentHash != contentHash) {
                throw new StoreException.Input(key, StoreException.InputProblem.BAD_CHECKSUM,
                        "Incorrect compressed checksum for "+stdMetadata.compression+" entry ("+bytes
                        +" bytes): got 0x"
                        +Integer.toHexString(stdMetadata.compressedContentHash)+", calculated to be 0x"
                        +Integer.toHexString(contentHash));
            }
        }
    }
    
    /**
     * Helper method used for reading as much data from the request as
     * possible, appending it in an off-heap buffer for further
     * processing.
     */
    protected int _readInBuffer(final OperationDiagnostics diag, final StorableKey key,
            InputStream input, byte[] readBuffer, StreamyBytesMemBuffer offHeap)
        throws IOException
    {
        final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
        try {
            while (true) {
                int count;
                try {
                    count = input.read(readBuffer);
                } catch (IOException e) { // probably will fail to write response too but...
                    throw new StoreException.IO(key, "Failed to read content to store (after "+offHeap.getTotalPayloadLength()
                            +" bytes)", e);
                }
                if (count < 0) { // got it all babe
                    return 0;
                }
                // can we append it in buffer?
                if (!offHeap.tryAppend(readBuffer, 0, count)) {
                    // if not, return to caller, indicating how much is left
                    return count;
                }
            }
        } finally {
            if (diag != null) {
                diag.addRequestReadTime(nanoStart, _timeMaster);
            }
        }
    }

    /**
     * Method called to actually write the entry metadata in local database.
     * 
     * @param operationTime Timestamp used as the "last-modified" timestamp in metadata;
     *   important as it determines last-modified traversal order for synchronization
     */
    protected StorableCreationResult _putPartitionedEntry(final StoreOperationSource source, final OperationDiagnostics diag,
            StorableKey key, final long operationTime,
            final StorableCreationMetadata stdMetadata, Storable storable,
            final OverwriteChecker allowOverwrites)
        throws IOException, StoreException
    {
        // NOTE: at this point request has been read, file written (if one needed),
        // so it's ok to assume rest is DB access.
        final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
        final StorableCreationResult result = _throttler.performPut(source,
                operationTime, key, storable,
                new StoreOperationCallback<StorableCreationResult>() {
            @Override
            public StorableCreationResult perform(long time, StorableKey key, final Storable newValue)
                throws IOException, StoreException
            {
                // blind update, insert-only are easy
                Boolean defaultOk = allowOverwrites.mayOverwrite(key);
                if (defaultOk != null) { // depends on entry in question...
                    if (defaultOk.booleanValue()) { // always ok, fine ("upsert")
                        return _writeMutex.partitionedWrite(time, key,
                                new PartitionedWriteMutex.Callback<StorableCreationResult>() {
                            @Override
                            public StorableCreationResult performWrite(StorableKey key) throws IOException, StoreException {
                                final long dbStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                                Storable oldValue =  _backend.putEntry(key, newValue);
                                if (diag != null) {
                                    diag.addDbAccess(nanoStart, dbStart, _timeMaster.nanosForDiagnostics());
                                }
                                return new StorableCreationResult(key, true, newValue, oldValue);
                            }
                        });
                    }
                    // strict "insert"
                    return _writeMutex.partitionedWrite(time, key,
                            new PartitionedWriteMutex.Callback<StorableCreationResult>() {
                        @Override
                        public StorableCreationResult performWrite(StorableKey key) throws IOException, StoreException {
                            final long dbStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                            Storable oldValue =  _backend.createEntry(key, newValue);
                            if (diag != null) {
                                diag.addDbAccess(nanoStart, dbStart, _timeMaster.nanosForDiagnostics());
                            }
                            if (oldValue == null) { // ok, succeeded
                                return new StorableCreationResult(key, true, newValue, null);
                            }
                            // fail: caller may need to clean up the underlying file
                            return new StorableCreationResult(key, false, newValue, oldValue);
                        }
                    });
                }
                // But if things depend on existence of old entry, or entries, trickier:
                return _writeMutex.partitionedWrite(time, key,
                        new PartitionedWriteMutex.Callback<StorableCreationResult>() {
                    @Override
                    public StorableCreationResult performWrite(StorableKey key) throws IOException, StoreException {
                        AtomicReference<Storable> oldEntryRef = new AtomicReference<Storable>();                       
                        final long dbStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                        boolean success = _backend.upsertEntry(key, newValue, allowOverwrites, oldEntryRef);
                        if (diag != null) {
                            diag.addDbAccess(nanoStart, dbStart, _timeMaster.nanosForDiagnostics());
                        }
                        if (!success) {
                        // fail due to existing entry
                            return new StorableCreationResult(key, false, newValue, oldEntryRef.get());
                        }
                        return new StorableCreationResult(key, true, newValue, oldEntryRef.get());
                    }
                });
            }
        });

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
    public StorableDeletionResult softDelete(final StoreOperationSource source, final OperationDiagnostics diag,
            StorableKey key,
            final boolean removeInlinedData, final boolean removeExternalData)
        throws IOException, StoreException
    {
        _checkClosed();
        final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
        Storable entry = _throttler.performSoftDelete(source,
                _timeMaster.currentTimeMillis(), key,
        new StoreOperationCallback<Storable>() {
            @Override
            public Storable perform(final long operationTime, StorableKey key, Storable value)
                throws IOException, StoreException
            {
                return _writeMutex.partitionedWrite(operationTime, key,
                        new PartitionedWriteMutex.Callback<Storable>() {
                    @Override
                    public Storable performWrite(StorableKey key) throws IOException, StoreException {
                        final long dbStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                        Storable value = _backend.findEntry(key);              
                        // First things first: if no entry, nothing to do
                        if (value == null) {
                            return null;
                        }
                        return _softDelete(source, diag, nanoStart, dbStart,
                                key, value, operationTime, removeInlinedData, removeExternalData);
                    }
                });
            }
        });
        return new StorableDeletionResult(key, entry);
    }
    
    @Override
    public StorableDeletionResult hardDelete(final StoreOperationSource source, final OperationDiagnostics diag,
            StorableKey key, final boolean removeExternalData)
        throws IOException, StoreException
    {
        _checkClosed();
        final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
        Storable entry = _throttler.performHardDelete(source,
                _timeMaster.currentTimeMillis(), key,
                new StoreOperationCallback<Storable>() {
            @Override
            public Storable perform(final long operationTime, StorableKey key, Storable value)
                throws IOException, StoreException
            {
                return _writeMutex.partitionedWrite(operationTime, key,
                        new PartitionedWriteMutex.Callback<Storable>() {
                    @Override
                    public Storable performWrite(StorableKey key) throws IOException, StoreException {
                        final long dbStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                        Storable value = _backend.findEntry(key);              
                        // First things first: if no entry, nothing to do
                        if (value == null) {
                            return null;
                        }
                        return _hardDelete(source, diag, nanoStart, dbStart,
                                key, value, removeExternalData);
                    }
                });
            }
        });
        return new StorableDeletionResult(key, entry);
    }

    protected Storable _softDelete(StoreOperationSource source, final OperationDiagnostics diag,
            final long nanoStart, final long dbStart,
            final StorableKey key, final Storable entry, final long currentTime,
            final boolean removeInlinedData, final boolean removeExternalData)
        throws IOException, StoreException
    {
        // Ok now... need to delete some data?
        boolean hasExternalToDelete = removeExternalData && entry.hasExternalData();
        if (!entry.isDeleted() || hasExternalToDelete
                || (removeInlinedData && entry.hasInlineData())) {
            File extFile = hasExternalToDelete ? entry.getExternalFile(_fileManager) : null;
            Storable modifiedEntry = _storableConverter.softDeletedCopy(key, entry, currentTime,
                    removeInlinedData, removeExternalData);
            _backend.ovewriteEntry(key, modifiedEntry);
            if (diag != null) {
                diag.addDbAccess(nanoStart, dbStart, _timeMaster.nanosForDiagnostics());
            }
            if (extFile != null) {
                _deleteBackingFile(key, extFile);
            }
            return modifiedEntry;
        }
        return entry;
    }

    protected Storable _hardDelete(StoreOperationSource source, final OperationDiagnostics diag,
            final long nanoStart, final long dbStart,
            StorableKey key, Storable entry,
            final boolean removeExternalData)
        throws IOException, StoreException
    {
        _backend.deleteEntry(key);
        if (diag != null) {
            diag.addDbAccess(nanoStart, dbStart, _timeMaster.nanosForDiagnostics());
        }
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
    public IterationResult iterateEntriesByKey(StoreOperationSource source, final OperationDiagnostics diag,
            final StorableKey firstKey,
            final StorableIterationCallback cb)
        throws StoreException
    {
        final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
        try {
            return _throttler.performList(source, _timeMaster.currentTimeMillis(),
                    new StoreOperationCallback<IterationResult>() {
                @Override
                public IterationResult perform(long operationTime, StorableKey key, Storable value)
                        throws IOException, StoreException {
                    final long dbStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                    try {
                        return _backend.iterateEntriesByKey(cb, firstKey);
                    } finally {
                        if (diag != null) {
                            diag.addDbAccess(nanoStart, dbStart, _timeMaster.nanosForDiagnostics());
                        }
                    }
                }
            });
        } catch (IOException e) {
            throw new StoreException.IO(firstKey, "Failed to iterate entries from "+firstKey+": "+e.getMessage(), e);
        }
    }

    @Override
    public IterationResult iterateEntriesAfterKey(StoreOperationSource source, final OperationDiagnostics diag,
            final StorableKey lastSeen,
            final StorableIterationCallback cb)
        throws StoreException
    {
        final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
        try {
            return _throttler.performList(source, _timeMaster.currentTimeMillis(),
            new StoreOperationCallback<IterationResult>() {
                @Override
                public IterationResult perform(long operationTime, StorableKey key, Storable value)
                        throws IOException, StoreException {
                    final long dbStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
                    // if we didn't get "lastSeen", same as regular method
                    try {
                        if (lastSeen == null) {
                            return _backend.iterateEntriesByKey(cb, null);
                        }
                        return _backend.iterateEntriesAfterKey(cb, lastSeen);
                    } finally {
                        if (diag != null) {
                            diag.addDbAccess(nanoStart, dbStart, _timeMaster.nanosForDiagnostics());
                        }
                    }
                }
            });
        } catch (IOException e) {
            throw new StoreException.IO(lastSeen, "Failed to iterate entries from "+lastSeen+": "+e.getMessage(), e);
        }
    }
    
    @Override
    public IterationResult iterateEntriesByModifiedTime(StoreOperationSource source, OperationDiagnostics diag,
            long firstTimestamp,
            StorableLastModIterationCallback cb)
        throws StoreException
    {
        final long nanoStart = (diag == null) ? 0L : _timeMaster.nanosForDiagnostics();
        try {
            return _backend.iterateEntriesByModifiedTime(cb, firstTimestamp);
        } finally {
            // no throttling for these (used only internally for now?), hence:
            if (diag != null) {
                diag.addDbAccess(nanoStart, nanoStart, _timeMaster.nanosForDiagnostics());
            }
        }
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
                hardDelete(source, null, key, true);
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
                hardDelete(source, null, key, true);
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

    /**
     * @return True if deletion was either not needed (no backing file), or
     *   if it succeeded; false if the file did not exist.
     */
    protected boolean _deleteBackingFile(StorableKey key, File extFile)
    {
        if (extFile == null) {
            return true;
        }
        try {
            boolean ok = extFile.delete();
            if (!ok) {
                LOG.warn("Failed to delete backing data file of key {}, path: {}",
                        key, extFile.getAbsolutePath());
            }
            return ok;
        } catch (Exception e) {
            LOG.warn("Failed to delete backing data file of key "+key+", path: "+extFile.getAbsolutePath(), e);
        }
        return false;
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
