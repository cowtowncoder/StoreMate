package com.fasterxml.storemate.store.backend;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.*;

public abstract class EmptyStoreTestBase extends BackendTestBase
{
//    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig)
    
    /**
     * Simple verification of basic invariants of a newly created
     * empty store
     */
    public void testVerifyEmpty() throws Exception
    {
        initTestLogging();
        
        StorableStore store = createStore("bdb-empty-1");

        // should be... empty.
        _verifyCounts(0L, store);

        StorableKey testKey = storableKey("bogus");
        
        assertNull(store.findEntry(StoreOperationSource.REQUEST, null, testKey));
        
        store.stop();
    }
}
