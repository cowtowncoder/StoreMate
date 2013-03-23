package com.fasterxml.storemate.backend.leveldb;

import java.io.File;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.MediumEntryTestBase;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class MediumEntryTest extends MediumEntryTestBase
{
    @Override
    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {
        return new LevelDBBuilder(storeConfig, new LevelDBConfig(new File(testRoot, "ldb"))).buildCreateAndInit();
    }
}
