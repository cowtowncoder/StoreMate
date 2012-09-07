package com.fasterxml.storemate.store.bdb;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;

import org.joda.time.DateTime;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.WithBytesAsArray;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.*;

public class SmallEntryTest extends StoreTestBase
{
    private long _date(int year, int month, int day)
    {
        return new DateTime(0L)
                .withYear(year)
                .withMonthOfYear(month)
                .withDayOfMonth(day)
                .withHourOfDay(0)
                .withMinuteOfHour(0)
                .withSecondOfMinute(0)
                .withMillisOfDay(0)
                .getMillis();
    }
    
    public void testSimpleSmall() throws IOException
    {
        initTestLogging();
        
        final long startTime = _date(2012, 6, 6);
        
        StorableStore store = createStore("bdb-small-1", startTime);

        assertEquals(0L, store.getEntryCount());
        assertEquals(0L, store.getIndexedCount());

        final StorableKey INTERNAL_KEY1 = storableKey("data/entry/1");
        final String SMALL_STRING = "Some data that we want to store -- small, gets inlined...";
        final byte[] SMALL_DATA = SMALL_STRING.getBytes("UTF-8");

        assertNull(store.findEntry(INTERNAL_KEY1));

        final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };

        // Ok: store a small entry:
        StorableCreationMetadata metadata = new StorableCreationMetadata(
        		/*existing compression*/ null,
        		calcChecksum32(SMALL_DATA), StoreConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(
        		INTERNAL_KEY1, new ByteArrayInputStream(SMALL_DATA),
        		metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());

        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1L, store.getEntryCount());
        assertEquals(1L, store.getIndexedCount());

        // Ok. Then, we should also be able to fetch it, right?
        Storable entry = store.findEntry(INTERNAL_KEY1);
        assertNotNull(entry);

        assertEquals(startTime, entry.getLastModified());
        assertTrue(entry.hasInlineData());
        assertFalse(entry.hasExternalData());
        // too short to be compressed:
        assertEquals(Compression.NONE, entry.getCompression());
        assertEquals(SMALL_DATA.length, entry.getStorageLength());
        // -1 means N/A, used when no compression is used:
        assertEquals(-1L, entry.getOriginalLength());

        // we passed no metadata, so:
        assertEquals(3, entry.getMetadataLength());
        byte[] actualMetadata1 = entry.withMetadata(WithBytesAsArray.instance);
        byte[] actualMetadata2 = entry.getMetadata().asBytes();
        assertArrayEquals(CUSTOM_METADATA_IN, actualMetadata1);
        assertArrayEquals(CUSTOM_METADATA_IN, actualMetadata2);
        
        // then let's verify inlined content itself
        byte[] actualContents1 = entry.getInlinedData().asBytes();
        byte[] actualContents2 = entry.withInlinedData(WithBytesAsArray.instance);
        assertArrayEquals(SMALL_DATA, actualContents1);
        assertArrayEquals(SMALL_DATA, actualContents2);
        
        store.stop();
    }

}
