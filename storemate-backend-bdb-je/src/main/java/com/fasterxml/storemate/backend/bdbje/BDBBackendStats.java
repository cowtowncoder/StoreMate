package com.fasterxml.storemate.backend.bdbje;

import com.fasterxml.storemate.store.backend.BackendStats;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.EnvironmentStats;

public class BDBBackendStats
    extends BackendStats
{
    public EnvironmentStats env;

    public DatabaseStats db;

    public BDBBackendStats() {
        super("bdb");
    }
}
