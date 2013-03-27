package com.fasterxml.storemate.store.backend;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.shared.util.WithBytesAsArray;
import com.fasterxml.storemate.store.*;

public abstract class DeleteEntryTestBase extends BackendTestBase
{
//    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig);

    /**
     * Test to check that deletion of small entries works as expected
     */
    public void testInlinedDeletion() throws Exception
    {
        final long startTime = _date(2012, 7, 9);
        StorableStore store = createStore("bdb-delete-small", startTime);
        _verifyCounts(0L, store);

        final StorableKey KEY1 = storableKey("data/entry/1");
        final StorableKey KEY2 = storableKey("data/entry/2");
        final byte[] SMALL_DATA = "Some data that we want to store -- small, gets inlined...".getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };

        assertNull(store.findEntry(KEY1));
        assertNull(store.findEntry(KEY2));

        // Ok: insert entries first
        StorableCreationMetadata metadata = new StorableCreationMetadata(null,
                calcChecksum32(SMALL_DATA), HashConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(KEY1, new ByteArrayInputStream(SMALL_DATA),
                metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());

        resp = store.insert(KEY2, new ByteArrayInputStream(SMALL_DATA),
                metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());
        
        _verifyCounts(2L, store);

        // Then deletions. First soft deletion, leaving inlined data intact
        StorableDeletionResult result = store.softDelete(KEY1, false, false);
        assertTrue(result.hadEntry());
        assertNotNull(result.getEntry());
        _verifyCounts(2L, store);
        // with soft deletion, we get modified instance, not original
        assertTrue(result.getEntry().isDeleted());
        assertTrue(result.getEntry().hasInlineData());

        // but let's see what is... "in store for us"
        Storable entry = store.findEntry(KEY1);
        assertNotNull(entry);
        assertTrue(entry.isDeleted());
        assertEquals(SMALL_DATA.length, entry.getInlineDataLength());
        assertArrayEquals(SMALL_DATA, entry.withInlinedData(WithBytesAsArray.instance));

        // second deletion, but here let's remove inlined data
        result = store.softDelete(KEY2, true, true);
        assertTrue(result.hadEntry());
        assertNotNull(result.getEntry());
        _verifyCounts(2L, store);
        assertEquals(0, result.getEntry().getInlineDataLength());
        assertArrayEquals(new byte[0], result.getEntry().withInlinedData(WithBytesAsArray.instance));

        // and then see how Store sees things:
        entry = store.findEntry(KEY2);
        assertNotNull(entry);
        assertTrue(entry.isDeleted());
        assertFalse(entry.hasInlineData());
        assertEquals(0, entry.getInlineDataLength());
        assertArrayEquals(new byte[0], entry.withInlinedData(WithBytesAsArray.instance));

        // so far so good. Then let's hard delete the second entry
        result = store.hardDelete(KEY2, true);
        assertTrue(result.hadEntry());
        _verifyCounts(1L, store);
        assertFalse(store.hasEntry(KEY2));

        result = store.hardDelete(KEY1, false); // second arg irrelevant, as we have no ext data
        assertTrue(result.hadEntry());
        _verifyCounts(0L, store);
        assertFalse(store.hasEntry(KEY2));
        
        store.stop();
    }
}