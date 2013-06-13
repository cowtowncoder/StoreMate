package com.fasterxml.storemate.store.backend;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.StorableLastModIterationCallback;

public abstract class LastModTestBase extends BackendTestBase
{
//    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {
    
   /**
    * Basic unit test that inserts a tiny entry (small enough not to
    * be compressed), verifies it can be read successfully.
    */
   public void testSimpleSmall() throws Exception
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
       StorableCreationResult resp = store.insert(StoreOperationSource.REQUEST,
               KEY1, new ByteArrayInputStream(SMALL_DATA),
               metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
       assertTrue(resp.succeeded());
       assertNull(resp.getPreviousEntry());
       _verifyCounts(1L, store);

       // then verify we can see it via iteration
       final AtomicInteger count = new AtomicInteger(0);
       store.iterateEntriesByModifiedTime(StoreOperationSource.REQUEST, 1L,
               new StorableLastModIterationCallback() {
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
           
       });
       assertEquals(1, count.get());

       // then add another entry; but in "wrong order"
       final long time2 = timeMaster.forceCurrentTimeMillis(startTime - 200L).currentTimeMillis();
       final StorableKey KEY2 = storableKey("data/entry/2");
       final byte[] SMALL_DATA2 = "Foo".getBytes("UTF-8");
       metadata = new StorableCreationMetadata(
               /*existing compression*/ null,
               calcChecksum32(SMALL_DATA2), HashConstants.NO_CHECKSUM);
       resp = store.insert(StoreOperationSource.REQUEST,
               KEY2, new ByteArrayInputStream(SMALL_DATA2),
               metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
        assertTrue(resp.succeeded());
        assertNull(resp.getPreviousEntry());
        _verifyCounts(2L, store);

        // and verify order
        final ArrayList<Long> timestamps = new ArrayList<Long>();
        final ArrayList<StorableKey> keys = new ArrayList<StorableKey>();
        store.iterateEntriesByModifiedTime(StoreOperationSource.REQUEST,
                0L, new StorableLastModIterationCallback() {
            long lastTimestamp;

            @Override
            public IterationAction verifyTimestamp(long timestamp) {
                timestamps.add(timestamp);
                lastTimestamp = timestamp;
                return IterationAction.PROCESS_ENTRY;
            }

            @Override
            public IterationAction verifyKey(StorableKey key) {
                keys.add(key);
                return IterationAction.PROCESS_ENTRY;
            }

            @Override
            public IterationAction processEntry(Storable entry) {
                assertEquals(lastTimestamp, entry.getLastModified());
                return IterationAction.PROCESS_ENTRY;
            }
        });
        assertEquals(2, timestamps.size());
        assertEquals(2, keys.size());
        assertEquals(Long.valueOf(time2), timestamps.get(0));
        assertEquals(Long.valueOf(startTime), timestamps.get(1));
        assertEquals(KEY2, keys.get(0));
        assertEquals(KEY1, keys.get(1));

        // finally, traverse partial:
        count.set(0);
        store.iterateEntriesByModifiedTime(StoreOperationSource.REQUEST,
                startTime-50L,
                new StorableLastModIterationCallback() {
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
        });
        assertEquals(1, count.get());
        
        store.stop();
   }

   // Longer test to verify that ordering is retained, similar to production usage
   public void testLongerSequence() throws Exception
   {
       final long startTime = _date(2012, 6, 6);
       TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
       final StorableStore store = createStore("bdb-lastmod-big", timeMaster);
       final byte[] CUSTOM_METADATA_IN = new byte[] { 1, 2, 3 };

       for (int i = 0; i < 100; ++i) {
           final StorableKey KEY1 = storableKey("data/entry/"+i);
           final byte[] SMALL_DATA = ("Data: "+i).getBytes("UTF-8");

           timeMaster.setCurrentTimeMillis(startTime + i * 5000); // 5 seconds in-between
           
           // Ok: store a small entry:
           StorableCreationMetadata metadata = new StorableCreationMetadata(
                   /*existing compression*/ null,
                   calcChecksum32(SMALL_DATA), HashConstants.NO_CHECKSUM);
           StorableCreationResult resp = store.insert(StoreOperationSource.REQUEST,
                   KEY1, new ByteArrayInputStream(SMALL_DATA),
                   metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
           assertTrue(resp.succeeded());
           assertNull(resp.getPreviousEntry());
           _verifyCounts(i+1, store);
       }

       // And then verify traversal order
       store.iterateEntriesByModifiedTime(StoreOperationSource.REQUEST, startTime-50L,
               new StorableLastModIterationCallback() {
           int index = 0;
           
           @Override
           public IterationAction verifyTimestamp(long timestamp) {
               assertEquals(startTime + index * 5000, timestamp);
               return IterationAction.PROCESS_ENTRY;
           }

           @Override
           public IterationAction verifyKey(StorableKey key) {
               assertEquals(storableKey("data/entry/"+index), key);
               // also: we better see the entry
               try {
                   assertTrue(store.hasEntry(StoreOperationSource.REQUEST, key));
               } catch (Exception e) {
                   fail("Prob with store"+e);
               }
               return IterationAction.PROCESS_ENTRY;
           }

           @Override
           public IterationAction processEntry(Storable entry) {
               assertEquals(startTime + index * 5000, entry.getLastModified());
               // and still have the entry
               try {
                   assertTrue(store.hasEntry(StoreOperationSource.REQUEST, entry.getKey()));
               } catch (Exception e) {
                   fail("Prob with store"+e);
               }

               ++index;
               return IterationAction.PROCESS_ENTRY;
           }
       });

       store.stop();
   }
}
