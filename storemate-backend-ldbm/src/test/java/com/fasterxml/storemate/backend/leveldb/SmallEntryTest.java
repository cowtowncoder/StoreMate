package com.fasterxml.storemate.backend.leveldb;

import java.io.File;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.SmallEntryTestBase;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class SmallEntryTest extends SmallEntryTestBase
{
    @Override
    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {
        return new LDBMBuilder(storeConfig, new LDBMConfig(new File(testRoot, "ldb"))).buildCreateAndInit();
    }
}
