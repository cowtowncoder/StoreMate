package com.fasterxml.storemate.backend.bdbje;

import java.io.*;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.*;

public class EmptyStoreTest extends BDBJETestBase
{
    /**
     * Simple verification of basic invariants of a newly created
     * empty store
     */
    public void testVerifyEmpty() throws IOException
    {
        initTestLogging();
        
        StorableStore store = createStore("bdb-empty-1");

        // should be... empty.
        assertEquals(0L, store.getEntryCount());
        assertEquals(0L, store.getIndexedCount());

        StorableKey testKey = storableKey("bogus");
        
        assertNull(store.findEntry(testKey));
        
        store.stop();
    }
}
