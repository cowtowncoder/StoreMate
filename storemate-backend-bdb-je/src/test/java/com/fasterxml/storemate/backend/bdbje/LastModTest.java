package com.fasterxml.storemate.backend.bdbje;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.StorableLastModIterationCallback;

public class LastModTest extends BDBJETestBase
{
   /**
    * Basic unit test that inserts a tiny entry (small enough not to
    * be compressed), verifies it can be read successfully.
    */
   public void testSimpleSmall() throws IOException
   {
       final long startTime = _date(2012, 6, 6);
       TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
       StorableStore store = createStore("bdb-lastmod", timeMaster);
       final StorableKey KEY1 = storableKey("data/entry/1");
       final byte[] SMALL_DATA = "Some data that we want to store -- small, gets inlined...".getBytes("UTF-8");
       final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };

       // Ok: store a small entry:
       StorableCreationMetadata metadata = new StorableCreationMetadata(
               /*existing compression*/ null,
               calcChecksum32(SMALL_DATA), HashConstants.NO_CHECKSUM);
       StorableCreationResult resp = store.insert(
               KEY1, new ByteArrayInputStream(SMALL_DATA),
               metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
       assertTrue(resp.succeeded());
       assertNull(resp.getPreviousEntry());
       assertEquals(1L, store.getEntryCount());
       assertEquals(1L, store.getIndexedCount());

       // then verify we can see it via iteration
       final AtomicInteger count = new AtomicInteger(0);
       store.iterateEntriesByModifiedTime(new StorableLastModIterationCallback() {
        @Override
        public IterationAction verifyTimestamp(long timestamp) {
            if (timestamp != startTime) {
                throw new IllegalStateException("Wrong timestamp, "+timestamp+", expected "+startTime);
            }
            return IterationAction.PROCESS_ENTRY;
        }

        @Override
        public IterationAction verifyKey(StorableKey key) {
            if (!key.equals(KEY1)) {
                throw new IllegalStateException("Wrong key: "+key);
            }
            return IterationAction.PROCESS_ENTRY;
        }

        @Override
        public IterationAction processEntry(Storable entry) {
            count.addAndGet(1);
            return IterationAction.PROCESS_ENTRY;
        }
           
       }, 1L);
       assertEquals(1, count.get());

       // then add another entry; but in "wrong order"
       final long time2 = timeMaster.forceCurrentTimeMillis(startTime - 200L).currentTimeMillis();
       final StorableKey KEY2 = storableKey("data/entry/2");
       final byte[] SMALL_DATA2 = "Foo".getBytes("UTF-8");
       metadata = new StorableCreationMetadata(
               /*existing compression*/ null,
               calcChecksum32(SMALL_DATA2), HashConstants.NO_CHECKSUM);
       resp = store.insert(
               KEY2, new ByteArrayInputStream(SMALL_DATA2),
               metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());
        assertEquals(2L, store.getEntryCount());
        assertEquals(2L, store.getIndexedCount());

        // and verify order
        final ArrayList<Long> timestamps = new ArrayList<Long>();
        store.iterateEntriesByModifiedTime(new StorableLastModIterationCallback() {
            @Override
            public IterationAction verifyTimestamp(long timestamp) {
                timestamps.add(timestamp);
                return IterationAction.PROCESS_ENTRY;
            }

            @Override
            public IterationAction verifyKey(StorableKey key) {
                return IterationAction.PROCESS_ENTRY;
            }

            @Override
            public IterationAction processEntry(Storable entry) {
                return IterationAction.PROCESS_ENTRY;
            }
        }, 0L);
        assertEquals(2, timestamps.size());
        assertEquals(Long.valueOf(time2), timestamps.get(0));
        assertEquals(Long.valueOf(startTime), timestamps.get(1));

        // finally, traverse partial:
        count.set(0);
        store.iterateEntriesByModifiedTime(new StorableLastModIterationCallback() {
            @Override
            public IterationAction verifyTimestamp(long timestamp) {
                if (timestamp != startTime) {
                    throw new IllegalStateException("Wrong timestamp, "+timestamp+", expected "+startTime);
                }
                return IterationAction.PROCESS_ENTRY;
            }

            @Override
            public IterationAction verifyKey(StorableKey key) {
                if (!key.equals(KEY1)) {
                    throw new IllegalStateException("Wrong key: "+key);
                }
                return IterationAction.PROCESS_ENTRY;
            }

            @Override
            public IterationAction processEntry(Storable entry) {
                count.addAndGet(1);
                return IterationAction.PROCESS_ENTRY;
            }
        }, startTime-50L);
        assertEquals(1, count.get());
        
        store.stop();
   }
}
