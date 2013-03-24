package com.fasterxml.storemate.store.backend;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.shared.util.WithBytesAsArray;

import com.fasterxml.storemate.store.*;

public abstract class SmallEntryTestBase extends BackendTestBase
{
//    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {

    /**
     * Basic unit test that inserts a tiny entry (small enough not to
     * be compressed), verifies it can be read successfully.
     */
    public void testSimpleSmall() throws IOException
    {
        final long startTime = _date(2012, 6, 6);
        StorableStore store = createStore("bdb-small-simple", startTime);
        _verifyEntryCount(0L, store);
        _verifyIndexCount(0L, store);

        final StorableKey KEY1 = storableKey("data/entry/1");
        final String SMALL_STRING = "Some data that we want to store -- small, gets inlined...";
        final byte[] SMALL_DATA = SMALL_STRING.getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };

        assertNull(store.findEntry(KEY1));

        // Ok: store a small entry:
        StorableCreationMetadata metadata = new StorableCreationMetadata(
        		/*existing compression*/ null,
        		calcChecksum32(SMALL_DATA), HashConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(
        		KEY1, new ByteArrayInputStream(SMALL_DATA),
        		metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());

        // can we count on this getting updated? Seems to be, FWIW
        _verifyEntryCount(1L, store);
        _verifyIndexCount(1L, store);

        // Ok. Then, we should also be able to fetch it, right?
        Storable entry = store.findEntry(KEY1);
        assertNotNull(entry);

        assertEquals(startTime, entry.getLastModified());
        assertTrue(entry.hasInlineData());
        assertFalse(entry.hasExternalData());
        // too short to be compressed:
        assertEquals(Compression.NONE, entry.getCompression());
        assertEquals(SMALL_DATA.length, entry.getStorageLength());
        // -1 means N/A, used when no compression is used:
        assertEquals(-1L, entry.getOriginalLength());

        // we passed bit of custom metadata, verify:
        _verifyMetadata(entry, CUSTOM_METADATA_IN);
        
        // then let's verify inlined content itself
        byte[] actualContents1 = entry.getInlinedData().asBytes();
        byte[] actualContents2 = entry.withInlinedData(WithBytesAsArray.instance);
        assertArrayEquals(SMALL_DATA, actualContents1);
        assertArrayEquals(SMALL_DATA, actualContents2);
        
        store.stop();
    }

    /**
     * Test to verify that "empty" content entries are handled correctly
     */
    public void testZeroLengthEntry() throws IOException
    {
        final long startTime = _date(2012, 6, 6);
        StorableStore store = createStore("bdb-small-empty", startTime);
        _verifyEntryCount(0L, store);
        _verifyIndexCount(0L, store);

        final StorableKey KEY1 = storableKey("data/entry/0");
        final byte[] NO_DATA = new byte[0];
        final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };

        assertNull(store.findEntry(KEY1));

        // Ok: store a small entry:
        StorableCreationMetadata metadata = new StorableCreationMetadata(
               /*existing compression*/ null,
               calcChecksum32(NO_DATA), HashConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(KEY1, new ByteArrayInputStream(NO_DATA),
               metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());

        _verifyEntryCount(1L, store);
        _verifyIndexCount(1L, store);

        Storable entry = store.findEntry(KEY1);
        assertNotNull(entry);

        assertEquals(startTime, entry.getLastModified());

        // no content, so:
        assertFalse(entry.hasInlineData());
        assertFalse(entry.hasExternalData());
        // too short to be compressed:
        assertEquals(Compression.NONE, entry.getCompression());
        assertEquals(NO_DATA.length, entry.getStorageLength());
        // -1 means N/A, used when no compression is used:
        assertEquals(-1L, entry.getOriginalLength());

        // we passed bit of custom metadata, verify:
        _verifyMetadata(entry, CUSTOM_METADATA_IN);
        
        // then let's verify inlined content itself
        byte[] actualContents1 = entry.getInlinedData().asBytes();
        byte[] actualContents2 = entry.withInlinedData(WithBytesAsArray.instance);
        assertArrayEquals(NO_DATA, actualContents1);
        assertArrayEquals(NO_DATA, actualContents2);
        
        store.stop();
    }
    
    /**
     * Slight variation of basic test: let's accept compressed
     * contents; verify that we are served compressed thing
     */
    public void testSmallAcceptGzip() throws IOException
    {
        final long startTime = _date(2012, 7, 7);
        StorableStore store = createStore("bdb-small-gzip", startTime);
        _verifyEntryCount(0L, store);
        _verifyIndexCount(0L, store);

        final StorableKey KEY1 = storableKey("data/entry/2");
        final String SMALL_STRING = this.biggerCompressibleData(400); // about 400 bytes
        final byte[] SMALL_DATA = SMALL_STRING.getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_IN = new byte[] { 4, 5, 6, 7 };

        assertNull(store.findEntry(KEY1));
        
        // Ok: insert compressible smallish (inlineable) entry:
        StorableCreationMetadata metadata = new StorableCreationMetadata(
                        /*existing compression*/ null,
                        calcChecksum32(SMALL_DATA), HashConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(KEY1, new ByteArrayInputStream(SMALL_DATA),
                        metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());
        _verifyEntryCount(1L, store);
        _verifyIndexCount(1L, store);

        // Ok. Then, we should also be able to fetch it, right?
        Storable entry = store.findEntry(KEY1);
        assertNotNull(entry);
        assertEquals(startTime, entry.getLastModified());
        assertTrue(entry.hasInlineData());
        assertFalse(entry.hasExternalData());
        // small, but not too small, so should be gzip'ed:
        assertEquals(Compression.GZIP, entry.getCompression());
        assertEquals(SMALL_DATA.length, entry.getOriginalLength());
        // hard-coded for now; but needs to be less...
        int exp = Compressors.gzipCompress(SMALL_DATA).length;
        assertEquals(exp, entry.getStorageLength());

        // we passed bit of custom metadata, verify:
        _verifyMetadata(entry, CUSTOM_METADATA_IN);
        
        // then let's verify inlined content itself
        byte[] actualContents = entry.getInlinedData().asBytes();
        byte[] uncomp = Compressors.gzipUncompress(actualContents);
        assertArrayEquals(SMALL_DATA, uncomp);
        
        store.stop();
    }

    /**
     * Test to verify handling of duplicate entry
     */
    public void testDuplicates() throws IOException
    {
        final long startTime = _date(2012, 7, 8);
        StorableStore store = createStore("bdb-small-dups", startTime);
        _verifyEntryCount(0L, store);
        _verifyIndexCount(0L, store);

        final StorableKey KEY1 = storableKey("data/entry/1");
        final byte[] SMALL_DATA = "Some smallish data...".getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_IN = new byte[] { (byte) 255 };

        assertNull(store.findEntry(KEY1));
        
        // Ok: insert entry
        StorableCreationMetadata metadata = new StorableCreationMetadata(
                        /*existing compression*/ null,
                        calcChecksum32(SMALL_DATA), HashConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(KEY1, new ByteArrayInputStream(SMALL_DATA),
                        metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());
        assertEquals(1L, store.getEntryCount());
        assertEquals(1L, store.getIndexedCount());

        // Ok: first, is ok to try to PUT again
        StorableCreationResult resp2 = store.insert(KEY1, new ByteArrayInputStream(SMALL_DATA),
                metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertFalse(resp2.succeeded());
        assertNotNull(resp2.getPreviousEntry());
        _verifyEntryCount(1L, store);
        _verifyIndexCount(1L, store);

        // and then verify entry
        Storable entry = store.findEntry(KEY1);
        assertNotNull(entry);
        assertEquals(startTime, entry.getLastModified());
        assertTrue(entry.hasInlineData());
        assertFalse(entry.hasExternalData());
        assertEquals(Compression.NONE, entry.getCompression());
        assertEquals(SMALL_DATA.length, entry.getStorageLength());

        // we passed bit of custom metadata, verify:
        _verifyMetadata(entry, CUSTOM_METADATA_IN);

        store.stop();
    }

    /**
     * Test to check that if we already have LZF, we don't even try to re-compress it
     */
    public void testSmallTextAlreadyLZF() throws IOException
    {
        final long startTime = _date(2012, 7, 9);
        StorableStore store = createStore("bdb-small-lzf", startTime);
        _verifyEntryCount(0L, store);
        _verifyIndexCount(0L, store);

        final StorableKey KEY1 = storableKey("data/small-LZF-1");
        final byte[] SMALL_DATA_ORIG = biggerCompressibleData(400).getBytes("UTF-8");
        final byte[] SMALL_DATA_LZF = Compressors.lzfCompress(SMALL_DATA_ORIG);
        final byte[] CUSTOM_METADATA_IN = new byte[] { (byte) 255 };

        assertNull(store.findEntry(KEY1));
        
        // Let's NOT indicate as compressed, should figure it out
        // and then verify that it is stored as if not compressed
        StorableCreationMetadata metadata = new StorableCreationMetadata(
                        /*existing compression*/ null,
                        calcChecksum32(SMALL_DATA_LZF), HashConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(KEY1, new ByteArrayInputStream(SMALL_DATA_LZF),
                        metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());
        _verifyEntryCount(1L, store);
        _verifyIndexCount(1L, store);

        // and then verify entry
        Storable entry = store.findEntry(KEY1);
        assertNotNull(entry);
        assertEquals(startTime, entry.getLastModified());
        assertTrue(entry.hasInlineData());
        assertFalse(entry.hasExternalData());
        // allegedly no compression (to disable any other automatic handling):
        assertEquals(Compression.NONE, entry.getCompression());
        assertEquals(SMALL_DATA_LZF.length, entry.getStorageLength());
        // caller can only specify original length if compression was enabled
        assertEquals(-1L, entry.getOriginalLength());

        // we passed bit of custom metadata, verify:
        _verifyMetadata(entry, CUSTOM_METADATA_IN);

        store.stop();
    }
    
    /**
     * Test to verify that trying to send non-LZF content, claiming to be LZF,
     * fails.
     */
    public void testSmallTextAllegedLZF() throws IOException
    {
        final long startTime = _date(2012, 7, 9);
        StorableStore store = createStore("bdb-small-lzffail", startTime);
        _verifyCounts(0L, store);

        final StorableKey KEY1 = storableKey("data/small-LZF-invalid");
        final byte[] SMALL_DATA = "ZV but not really LZF".getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_IN = new byte[] { 77, 65, 13, 19, 0, 0 };

        assertNull(store.findEntry(KEY1));

        // Ok, then with data that claims to be LZF, but is not. Should be caught:
        StorableCreationMetadata metadata = new StorableCreationMetadata(
                Compression.LZF,
                calcChecksum32(SMALL_DATA), HashConstants.NO_CHECKSUM);
        try {
            /*StorableCreationResult resp =*/ store.insert(KEY1, new ByteArrayInputStream(SMALL_DATA),
                metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
            fail("Should have failed to add invalid data");
        } catch (StoreException e) {
            verifyException(e, "Invalid compression");
        }
        _verifyCounts(0L, store);

        store.stop();
    }
}
