package com.fasterxml.storemate.store.bdb;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.*;

public class SmallEntryTest extends StoreTestBase
{
    public void testSimpleSmall() throws IOException
    {
    	final long startTime = 1234L;
        initTestLogging();
        
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
        		null, calcChecksum32(SMALL_DATA), StoreConstants.NO_CHECKSUM);
        StorableCreationResult resp = store.insert(
        		INTERNAL_KEY1, new ByteArrayInputStream(SMALL_DATA),
        		metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());

        /*
        assertEquals(200, response.getStatus());
        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1, entries.getEntryCount());

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, null, null, creationTime);
        assertEquals(200, response.getStatus());

        // let's verify it then; small request...
        assertTrue(response.hasStreamingContent());
        assertTrue(response.hasInlinedData());
        byte[] data = collectOutput(response);
        assertEquals(SMALL_STRING, new String(data, "UTF-8"));

        // false->do NOT (yet) update last-accessed:
        EntryMetadata entry = entries.findEntry(INTERNAL_KEY1, false);
        assertNotNull(entry);
        // too small to be compressed, so:
        assertEquals(Compression.NONE.asByte(), entry.isCompressed);
        assertNull(entry.path);
        assertNotNull(entry.inlinedData);
        assertEquals(SMALL_DATA.length, entry.inlinedData.length);
        assertEquals(creationTime, entry.getCreationTime());
        assertEquals(creationTime, entry.getInsertionTime());
        assertEquals(0L, entry.lastAccess);

        // one more access; this time to modify last accessed
        long accessTime = creationTime + 999L;
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, null, null, accessTime);
        assertEquals(200, response.getStatus());

        // true->should update last-accessed timestamp
        entry = entries.findEntry(INTERNAL_KEY1, true);
        assertNotNull(entry);
        assertEquals(accessTime, entry.lastAccess);
        */

        store.stop();
    }

}
