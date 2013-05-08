package com.fasterxml.storemate.backend.leveldb;

import java.io.File;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.LastModTestBase;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class LastModTest extends LastModTestBase
{
    @Override
    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {
        return new LMDBBuilder(storeConfig, new LMDBConfig(new File(testRoot, "ldb"))).buildCreateAndInit();
    }
}
