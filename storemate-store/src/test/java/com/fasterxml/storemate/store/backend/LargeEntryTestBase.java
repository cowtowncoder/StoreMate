package com.fasterxml.storemate.store.backend;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.File;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.ThrottlingByteArrayInputStream;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.store.*;

public abstract class LargeEntryTestBase extends BackendTestBase
{
//    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {
    final long START_TIME = _date(2012, 7, 9);
    
    /**
     * Simple unit test to verify handling of off-lined ("streaming")
     * insertion of entries.
     */
    public void testSimple100k() throws Exception
    {
        StorableStore store = createStore("db-large-simple-100k", START_TIME);
        _testLarger(START_TIME, store, 100 * 1024);
    }

    public void testSimple5Megs() throws Exception
    {
        StorableStore store = createStore("db-large-simple-5megs", START_TIME);
        _testLarger(START_TIME, store, 5 * 1024 * 1024);
    }

    public void testLargerAlreadyLZF() throws Exception
    {
        StorableStore store = createStore("bdb-large-lzf", START_TIME);
        final StorableKey KEY1 = storableKey("data/small-LZF-1");
        final byte[] DATA_ORIG = biggerCompressibleData(480 * 1000).getBytes("UTF-8");
        final byte[] DATA_LZF = Compressors.lzfCompress(DATA_ORIG);
        final byte[] CUSTOM_METADATA_IN = new byte[] { (byte) 255 };

        try {
            _verifyCounts(0L, store);
    
            assertNull(store.findEntry(StoreOperationSource.REQUEST, null, KEY1));
            StorableCreationMetadata metadata = new StorableCreationMetadata(Compression.LZF,
                            calcChecksum32(DATA_ORIG), calcChecksum32(DATA_LZF));
            metadata.uncompressedSize = DATA_ORIG.length;
            StorableCreationResult resp = store.insert(StoreOperationSource.REQUEST, null,
                    KEY1, new ByteArrayInputStream(DATA_LZF),
                    metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
            assertTrue(resp.succeeded());
            assertNull(resp.getPreviousEntry());
            _verifyCounts(1L, store);
    
            // and then verify entry
            Storable entry = store.findEntry(StoreOperationSource.REQUEST, null, KEY1);
            assertNotNull(entry);
            assertEquals(START_TIME, entry.getLastModified());
            assertFalse(entry.hasInlineData());
            assertTrue(entry.hasExternalData());
    
            assertEquals(Compression.LZF, entry.getCompression());
            assertEquals(DATA_LZF.length, entry.getStorageLength());
    
            assertEquals(DATA_ORIG.length, entry.getOriginalLength());
    
            _verifyMetadata(entry, CUSTOM_METADATA_IN);
        } finally {
            store.stop();
        }
    }
    
    private void _testLarger(long startTime, StorableStore store,
            int origSize) throws Exception
    {
        _verifyCounts(0L, store);

        final StorableKey KEY1 = storableKey("data/1");
        final String DATA_STRING = biggerCompressibleData(origSize);
        final byte[] DATA = DATA_STRING.getBytes("UTF-8");

        final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };
        final byte[] COMPRESSED_DATA = Compressors.lzfCompress(DATA);

        assertNull(store.findEntry(StoreOperationSource.REQUEST, null, KEY1));

        // then try adding said entry
        StorableCreationMetadata metadata0 = new StorableCreationMetadata(
                /*existing compression*/ null,
                calcChecksum32(DATA), HashConstants.NO_CHECKSUM);
        
        // important: throttle input reading, to force chunking (and maybe tease out bugs)
        StorableCreationResult resp = store.insert(StoreOperationSource.REQUEST, null,
                KEY1, new ThrottlingByteArrayInputStream(DATA, 97),
                metadata0.clone(), ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());
        _verifyCounts(1L, store);

        // and then verify entry
        Storable entry = store.findEntry(StoreOperationSource.REQUEST, null, KEY1);
        assertNotNull(entry);
        assertEquals(startTime, entry.getLastModified());
        // big enough so it can't be inlined:
        assertFalse(entry.hasInlineData());
        assertTrue(entry.hasExternalData());
        // allegedly no compression (to disable any other automatic handling):
        assertEquals(Compression.LZF, entry.getCompression());
        int expCompressedSize = COMPRESSED_DATA.length;
        assertEquals(expCompressedSize, entry.getStorageLength());
        assertEquals(DATA.length, entry.getOriginalLength());
        _verifyHash(COMPRESSED_DATA, entry.getCompressedHash(), "Compressed LZF data");

        // we passed bit of custom metadata, verify:
        _verifyMetadata(entry, CUSTOM_METADATA_IN);

        // and external path should also work; verify some naming aspects
        // (where 'Z' is compression indicator" and 123 sequence number)

        File file = entry.getExternalFile(store.getFileManager());
        assertNotNull(file);
        String path = file.getAbsolutePath();

        String filename = path.substring(path.lastIndexOf('/')+1);
        String suffix = filename.substring(filename.lastIndexOf('.'));
        assertEquals(".L", suffix);
        assertEquals("0000:data_1", filename.substring(0, filename.lastIndexOf('.')));

        // but as importantly, check that file is there and contains data...
        assertTrue(file.exists());

        byte[] fileData = readFile(file);
        assertArrayEquals(COMPRESSED_DATA, fileData);

        /* Let's also verify checksum handling: should have one for compressed
         * data at least...
         */
        _verifyHash(fileData, entry.getCompressedHash(), "file checksum for large data");
        
        // Actually, let's also verify handling of dups...

        StorableCreationResult resp2 = store.insert(StoreOperationSource.REQUEST, null,
                KEY1, new ThrottlingByteArrayInputStream(DATA, 77),
                metadata0.clone(), ByteContainer.simple(CUSTOM_METADATA_IN));
        assertFalse(resp2.succeeded());
        assertNotNull(resp2.getPreviousEntry());
        _verifyCounts(1L, store);
        // should we see if multiple files exist?
        
        store.stop();
    }
}
