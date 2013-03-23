package com.fasterxml.storemate.backend.leveldb;

import java.io.*;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.EmptyStoreTestBase;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class EmptyStoreTest extends EmptyStoreTestBase
{
    @Override
    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {
        return new LevelDBBuilder(storeConfig, new LevelDBConfig(new File(testRoot, "ldb"))).buildCreateAndInit();
    }
}
