package com.fasterxml.storemate.backend.bdbje;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;

import com.fasterxml.storemate.store.*;

public class LargeEntryTest extends BDBJETestBase
{
    /**
     * Simple unit test to verify handling of off-lined ("streaming")
     * insertion of entries.
     */
    public void testSimpleLarge() throws IOException
    {
        final long startTime = _date(2012, 7, 9);
        StorableStore store = createStore("bdb-medium-simple", startTime);
        assertEquals(0L, store.getEntryCount());
        assertEquals(0L, store.getIndexedCount());

        final StorableKey KEY1 = storableKey("data/1");
        int origSize = StoreConfig.DEFAULT_MIN_PAYLOAD_FOR_STREAMING + 500;
        final String DATA_STRING = biggerCompressibleData(origSize);
        final byte[] DATA = DATA_STRING.getBytes("UTF-8");

        final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };
        final byte[] COMPRESSED_DATA = Compressors.lzfCompress(DATA);

        assertNull(store.findEntry(KEY1));

        // then try adding said entry
        StorableCreationMetadata metadata0 = new StorableCreationMetadata(
                /*existing compression*/ null,
                calcChecksum32(DATA), StoreConstants.NO_CHECKSUM);
        
        StorableCreationResult resp = store.insert(KEY1, new ByteArrayInputStream(DATA),
                metadata0.clone(), ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());
        assertEquals(1L, store.getEntryCount());
        assertEquals(1L, store.getIndexedCount());

        // and then verify entry
        Storable entry = store.findEntry(KEY1);
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

        // we passed bit of custom metadata, verify:
        _verifyMetadata(entry, CUSTOM_METADATA_IN);

        // and external path should also work; verify some naming aspects
        // (where 'Z' is compression indicator" and 123 sequence number)

        File file = entry.getExternalFile(store.getFileManager());
        assertNotNull(file);
        String path = file.getAbsolutePath();

        String filename = path.substring(path.lastIndexOf('/')+1);
        String suffix = filename.substring(filename.lastIndexOf('.'));
        assertEquals(".0L", suffix);
        assertEquals("data_1", filename.substring(0, filename.lastIndexOf('.')));

        // but as importantly, check that file is there and contains data...
        assertTrue(file.exists());

        assertArrayEquals(COMPRESSED_DATA, readFile(file));

        // Actually, let's also verify handling of dups...

        StorableCreationResult resp2 = store.insert(KEY1, new ByteArrayInputStream(DATA),
                metadata0.clone(), ByteContainer.simple(CUSTOM_METADATA_IN));
        assertFalse(resp2.succeeded());
        assertNotNull(resp2.getPreviousEntry());
        assertEquals(1L, store.getEntryCount());
        assertEquals(1L, store.getIndexedCount());
        // should we see if multiple files exist?
        
        store.stop();
    }

    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */

    @Override
    public void setUp() {
        initTestLogging();
    }
}
