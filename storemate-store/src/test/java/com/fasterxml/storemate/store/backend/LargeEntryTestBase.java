package com.fasterxml.storemate.store.backend;

import static org.junit.Assert.assertArrayEquals;

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
    
    /**
     * Simple unit test to verify handling of off-lined ("streaming")
     * insertion of entries.
     */
    public void testSimpleLarge() throws Exception
    {
        final long startTime = _date(2012, 7, 9);
        StorableStore store = createStore("bdb-medium-simple", startTime);
        _verifyCounts(0L, store);

        final StorableKey KEY1 = storableKey("data/1");
        int origSize = StoreConfig.DEFAULT_MIN_PAYLOAD_FOR_STREAMING + 500;
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
        StorableCreationResult resp = store.insert(StoreOperationSource.REQUEST,
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

        StorableCreationResult resp2 = store.insert(StoreOperationSource.REQUEST,
                KEY1, new ThrottlingByteArrayInputStream(DATA, 77),
                metadata0.clone(), ByteContainer.simple(CUSTOM_METADATA_IN));
        assertFalse(resp2.succeeded());
        assertNotNull(resp2.getPreviousEntry());
        _verifyCounts(1L, store);
        // should we see if multiple files exist?
        
        store.stop();
    }
}
