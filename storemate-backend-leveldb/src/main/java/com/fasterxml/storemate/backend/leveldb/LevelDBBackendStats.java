package com.fasterxml.storemate.backend.leveldb;

import java.util.*;

import com.fasterxml.storemate.store.backend.BackendStats;
import com.fasterxml.storemate.store.backend.BackendStatsConfig;

public class LevelDBBackendStats
    extends BackendStats
{
    public Map<String,Object> stats;

    public LevelDBBackendStats() { this(null, 0L, null); }

    public LevelDBBackendStats(BackendStatsConfig config, long creationTime,
            Map<String,Object> src)
    {
        super("leveldb", creationTime, config);
        stats = src;
    }

    @Override
    public Map<String,Object> extraStats(Map<String,Object> base) {
        if (stats != null) {
            base.put("stats", stats);
        }
        return base;
    }
}
