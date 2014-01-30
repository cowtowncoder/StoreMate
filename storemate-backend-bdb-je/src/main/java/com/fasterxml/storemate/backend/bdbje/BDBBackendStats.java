package com.fasterxml.storemate.backend.bdbje;

import java.util.Map;

import com.fasterxml.storemate.store.backend.BackendStats;
import com.fasterxml.storemate.store.backend.BackendStatsConfig;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.EnvironmentStats;

public class BDBBackendStats
    extends BackendStats
{
    public EnvironmentStats env;

    public DatabaseStats db;

    private BDBBackendStats() { // only for deserialization
        super("bdb", 0L, null);
    }
    
    public BDBBackendStats(BackendStatsConfig config, long creationTime) {
        super("bdb", creationTime, config);
    }

    public BDBBackendStats(BDBBackendStats src) {
        super(src);
        env = src.env;
        db = src.db;
    }

    @Override
    public Map<String,Object> extraStats(Map<String,Object> base) {
        base.put("env", env);
        base.put("db", db);
        return base;
    }
}
