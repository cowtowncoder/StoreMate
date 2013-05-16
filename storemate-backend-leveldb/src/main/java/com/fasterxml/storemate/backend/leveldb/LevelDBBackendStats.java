package com.fasterxml.storemate.backend.leveldb;

import java.util.*;

import com.fasterxml.storemate.store.backend.BackendStats;

public class LevelDBBackendStats
    extends BackendStats
{
    public Map<String,Object> stats;

    public LevelDBBackendStats() { this(null); }
    public LevelDBBackendStats(Map<String,Object> src)
    {
        super("leveldb");
        stats = src;
    }
}
