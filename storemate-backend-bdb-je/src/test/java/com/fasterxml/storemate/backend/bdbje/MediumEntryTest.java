package com.fasterxml.storemate.backend.bdbje;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.store.*;

public class MediumEntryTest extends BDBJETestBase
{
    // Test to use GZIP
    public void testMediumFileWithGZIP() throws IOException
    {
        final long startTime = _date(2012, 7, 9);
        StorableStore store = createStore("bdb-medium-simple", startTime);
        assertEquals(0L, store.getEntryCount());
        assertEquals(0L, store.getIndexedCount());

        final StorableKey KEY1 = storableKey("data/1");
        int origSize = StoreConfig.DEFAULT_MAX_FOR_GZIP - 100;
        final String DATA_STRING = biggerCompressibleData(origSize);
        final byte[] DATA = DATA_STRING.getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };
        final byte[] GZIPPED_DATA = Compressors.gzipCompress(DATA);

        assertNull(store.findEntry(KEY1));

        // then try adding said entry
        StorableCreationMetadata metadata = new StorableCreationMetadata(
                /*existing compression*/ null,
                calcChecksum32(DATA), HashConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(KEY1, new ByteArrayInputStream(DATA),
                metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
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
        assertEquals(Compression.GZIP, entry.getCompression());
        int expCompressedSize = GZIPPED_DATA.length;
        assertEquals(expCompressedSize, entry.getStorageLength());
        assertEquals(DATA.length, entry.getOriginalLength());
        _verifyHash(DATA, entry.getContentHash(), "original content hash");
        _verifyHash(GZIPPED_DATA, entry.getCompressedHash(), "compressed hash");
        
        // we passed bit of custom metadata, verify:
        _verifyMetadata(entry, CUSTOM_METADATA_IN);

        // and external path should also work; verify some naming aspects
        // (where 'Z' is compression indicator" and 123 sequence number)

        File file = entry.getExternalFile(store.getFileManager());
        assertNotNull(file);
        String path = file.getAbsolutePath();

        String filename = path.substring(path.lastIndexOf('/')+1);
        String suffix = filename.substring(filename.lastIndexOf('.'));
        assertEquals(".Z", suffix);
        assertEquals("0000:data_1", filename.substring(0, filename.lastIndexOf('.')));

        // but as importantly, check that file is there and contains data...
        assertTrue(file.exists());

        assertArrayEquals(GZIPPED_DATA, readFile(file));

        store.stop();
    }

    public void testMediumFileWithLZF() throws IOException
    {
        final long startTime = _date(2012, 7, 9);
        StorableStore store = createStore("bdb-medium-lzf", startTime);
        assertEquals(0L, store.getEntryCount());
        assertEquals(0L, store.getIndexedCount());

        final StorableKey KEY1 = storableKey("data/1");
        int origSize = StoreConfig.DEFAULT_MAX_FOR_GZIP + 2000;
        final String DATA_STRING = biggerCompressibleData(origSize);
        final byte[] DATA = DATA_STRING.getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };
        final byte[] LZF_DATA = Compressors.lzfCompress(DATA);

        assertNull(store.findEntry(KEY1));

        // then try adding said entry
        StorableCreationMetadata metadata = new StorableCreationMetadata(
                /*existing compression*/ null,
                calcChecksum32(DATA), HashConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(KEY1, new ByteArrayInputStream(DATA),
                metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
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
        int expCompressedSize = LZF_DATA.length;
        assertEquals(expCompressedSize, entry.getStorageLength());
        assertEquals(DATA.length, entry.getOriginalLength());
        _verifyHash(DATA, entry.getContentHash(), "original content hash");
        _verifyHash(LZF_DATA, entry.getCompressedHash(), "compressed hash");

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

        assertArrayEquals(LZF_DATA, readFile(file));

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
