package com.fasterxml.storemate.backend.bdbje;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.util.WithBytesAsArray;
import com.fasterxml.storemate.store.*;

public class DeleteEntryTest extends BDBJETestBase
{
    /**
     * Test to check that deletion of small entries works as expected
     */
    public void testInlinedDeletion() throws IOException
    {
        final long startTime = _date(2012, 7, 9);
        StorableStore store = createStore("bdb-delete-small", startTime);
        assertEquals(0L, store.getEntryCount());
        assertEquals(0L, store.getIndexedCount());

        final StorableKey KEY1 = storableKey("data/entry/1");
        final StorableKey KEY2 = storableKey("data/entry/2");
        final byte[] SMALL_DATA = "Some data that we want to store -- small, gets inlined...".getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };

        assertNull(store.findEntry(KEY1));
        assertNull(store.findEntry(KEY2));

        // Ok: insert entries first
        StorableCreationMetadata metadata = new StorableCreationMetadata(null,
                calcChecksum32(SMALL_DATA), StoreConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(KEY1, new ByteArrayInputStream(SMALL_DATA),
                metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());

        resp = store.insert(KEY2, new ByteArrayInputStream(SMALL_DATA),
                metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());
        
        assertEquals(2L, store.getEntryCount());
        assertEquals(2L, store.getIndexedCount());

        // Then deletions. First soft deletion, leaving inlined data intact
        StorableDeletionResult result = store.softDelete(KEY1, false, false);
        assertTrue(result.hadEntry());
        assertNotNull(result.getEntry());
        assertEquals(2L, store.getEntryCount());
        assertEquals(2L, store.getIndexedCount());
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
        assertEquals(2L, store.getEntryCount());
        assertEquals(2L, store.getIndexedCount());
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
        assertEquals(1L, store.getEntryCount());
        assertEquals(1L, store.getIndexedCount());
        assertFalse(store.hasEntry(KEY2));

        result = store.hardDelete(KEY1, false); // second arg irrelevant, as we have no ext data
        assertTrue(result.hadEntry());
        assertEquals(0L, store.getEntryCount());
        assertEquals(0L, store.getIndexedCount());
        assertFalse(store.hasEntry(KEY2));
        
        store.stop();
    }
}