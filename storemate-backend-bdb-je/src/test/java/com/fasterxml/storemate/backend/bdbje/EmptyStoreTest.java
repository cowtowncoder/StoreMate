package com.fasterxml.storemate.backend.bdbje;

import java.io.*;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.backend.EmptyStoreTestBase;
import com.fasterxml.storemate.store.backend.StoreBackend;

public class EmptyStoreTest extends EmptyStoreTestBase
{
    @Override
    protected StoreBackend createBackend(File testRoot, StoreConfig storeConfig) {
        return new BDBJEBuilder(storeConfig, new BDBJEConfig(new File(testRoot, "bdb"))).buildCreateAndInit();
    }
}
