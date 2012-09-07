package com.fasterxml.storemate.store.bdb;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;

import org.joda.time.DateTime;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.WithBytesAsArray;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.store.*;

public class SmallEntryTest extends StoreTestBase
{
    public void testSimpleSmall() throws IOException
    {
        initTestLogging();
        final long startTime = _date(2012, 6, 6);
        StorableStore store = createStore("bdb-small-simple", startTime);
        assertEquals(0L, store.getEntryCount());
        assertEquals(0L, store.getIndexedCount());

        final StorableKey KEY1 = storableKey("data/entry/1");
        final String SMALL_STRING = "Some data that we want to store -- small, gets inlined...";
        final byte[] SMALL_DATA = SMALL_STRING.getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };

        assertNull(store.findEntry(KEY1));

        // Ok: store a small entry:
        StorableCreationMetadata metadata = new StorableCreationMetadata(
        		/*existing compression*/ null,
        		calcChecksum32(SMALL_DATA), StoreConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(
        		KEY1, new ByteArrayInputStream(SMALL_DATA),
        		metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());

        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1L, store.getEntryCount());
        assertEquals(1L, store.getIndexedCount());

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
     * Slight variation of basic test: let's accept compressed
     * contents; verify that we are served compressed thing
     */
    public void testSmallAcceptGzip() throws IOException
    {
        initTestLogging();
        final long startTime = _date(2012, 7, 7);
        StorableStore store = createStore("bdb-small-gzip", startTime);
        assertEquals(0L, store.getEntryCount());
        assertEquals(0L, store.getIndexedCount());

        final StorableKey KEY1 = storableKey("data/entry/2");
        final String SMALL_STRING = this.biggerCompressibleData(400); // about 400 bytes
        final byte[] SMALL_DATA = SMALL_STRING.getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_IN = new byte[] { 4, 5, 6, 7 };

        assertNull(store.findEntry(KEY1));
        
        // Ok: insert compressible smallish (inlineable) entry:
        StorableCreationMetadata metadata = new StorableCreationMetadata(
                        /*existing compression*/ null,
                        calcChecksum32(SMALL_DATA), StoreConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(KEY1, new ByteArrayInputStream(SMALL_DATA),
                        metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());

        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1L, store.getEntryCount());
        assertEquals(1L, store.getIndexedCount());

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
    
    protected void _verifyMetadata(Storable entry, byte[] inputMetadata)
    {
        assertEquals(inputMetadata.length, entry.getMetadataLength());
        byte[] actualMetadata1 = entry.withMetadata(WithBytesAsArray.instance);
        byte[] actualMetadata2 = entry.getMetadata().asBytes();
        assertArrayEquals(inputMetadata, actualMetadata1);
        assertArrayEquals(inputMetadata, actualMetadata2);
    }
}
