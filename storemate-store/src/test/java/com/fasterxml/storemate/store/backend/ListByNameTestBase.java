package com.fasterxml.storemate.store.backend;

import java.io.ByteArrayInputStream;
import java.util.*;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.StorableIterationCallback;

public abstract class ListByNameTestBase extends BackendTestBase
{
//    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {
    
    private static class Callback extends StorableIterationCallback
    {
        private final HashSet<StorableKey> _keys;
        
        public int count;
        
        public Callback(StorableKey... keys) {
            _keys = new HashSet<StorableKey>(Arrays.asList(keys));
        }            
        
        @Override
        public IterationAction verifyKey(StorableKey key) {
            if (!_keys.contains(key)) {
                throw new IllegalStateException("Wrong key: "+key);
            }
            return IterationAction.PROCESS_ENTRY;
        }

        @Override
        public IterationAction processEntry(Storable entry) {
            ++count;
            return IterationAction.PROCESS_ENTRY;
        }
    }

    public void testSimpleSmall() throws Exception
    {
        final long startTime = _date(2012, 6, 6);
        TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
        StorableStore store = createStore("bdb-listbyname", timeMaster);

        final byte[] CUSTOM_METADATA_IN = new byte[0];

        final StorableKey KEY1 = storableKey("entryA");
        final StorableKey KEY2 = storableKey("entryB");
        final StorableKey KEY3 = storableKey("entryC");
        final StorableKey KEY4 = storableKey("entryD");
        
        /* Here we will use 4 keys, but insert them in different order, to
         * avoid relying on physical insertion order
         */
        for (StorableKey key : new StorableKey[] { KEY2, KEY4, KEY3, KEY1 } ) {
            StorableCreationMetadata metadata = new StorableCreationMetadata(
                    /*existing compression*/ null,
                    0, HashConstants.NO_CHECKSUM);
            StorableCreationResult resp = store.insert(StoreOperationSource.REQUEST,
                    key, new ByteArrayInputStream(key.asBytes()),
                    metadata, ByteContainer.simple(CUSTOM_METADATA_IN));
            assertTrue(resp.succeeded());
            assertNull(resp.getPreviousEntry());
        }

        // and verify invariants:
        _verifyCounts(4L, store);

        // then verify we can see them via various kinds of iteration
        Callback cb = new Callback(KEY1, KEY2, KEY3, KEY4);
        store.iterateEntriesByKey(StoreOperationSource.REQUEST, null, cb);
        assertEquals(4, cb.count);

        // then try partial traversal, inclusive first:
        cb = new Callback(KEY1, KEY2, KEY3, KEY4);
        store.iterateEntriesByKey(StoreOperationSource.REQUEST, KEY2, cb);
        assertEquals(3, cb.count);
        cb = new Callback(KEY1, KEY2, KEY3, KEY4);
        store.iterateEntriesByKey(StoreOperationSource.REQUEST, KEY4, cb);
        assertEquals(1, cb.count);

        // and exclusive
        cb = new Callback(KEY1, KEY2, KEY3, KEY4);
        store.iterateEntriesAfterKey(StoreOperationSource.REQUEST, KEY2, cb);
        assertEquals(2, cb.count);
        cb = new Callback(KEY1, KEY2, KEY3, KEY4);
        store.iterateEntriesAfterKey(StoreOperationSource.REQUEST, KEY4, cb);
        assertEquals(0, cb.count);
        
        store.stop();
    }

}
