package com.fasterxml.storemate.backend.bdbje;

import java.io.File;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.MediumEntryTestBase;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class MediumEntryTest extends MediumEntryTestBase
{
    @Override
    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {
        return new BDBJEBuilder(storeConfig, new BDBJEConfig(new File(testRoot, "bdb"))).buildCreateAndInit();
    }
}
