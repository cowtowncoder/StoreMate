package com.fasterxml.storemate.backend.bdbje;

import java.io.File;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.DeleteEntryTestBase;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class DeleteEntryTest extends DeleteEntryTestBase
{
    @Override
    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {
        return new BDBJEBuilder(storeConfig, new BDBJEConfig(new File(testRoot, "bdb"))).buildCreateAndInit();
    }
}