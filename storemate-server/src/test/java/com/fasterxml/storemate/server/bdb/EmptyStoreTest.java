package com.fasterxml.storemate.server.bdb;

import java.io.File;
import java.io.IOException;

import com.fasterxml.storemate.shared.TimeMaster;

import com.fasterxml.storemate.server.ServerTestBase;
import com.fasterxml.storemate.server.TimeMasterForSimpleTesting;
import com.fasterxml.storemate.server.file.*;

public class EmptyStoreTest extends ServerTestBase
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

        store.stop();
    }
}
