package com.fasterxml.storemate.backend.leveldb;

import java.io.File;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.ListByNameTestBase;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class ListByNameTest extends ListByNameTestBase
{
    @Override
    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {
        return new LDBMBuilder(storeConfig, new LDBMConfig(new File(testRoot, "ldb"))).buildCreateAndInit();
    }
}
