package com.fasterxml.storemate.store.impl;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.StorableIterationCallback;

/**
 * Helper class used for estimating number of tombstones in the
 * store.
 */
class TombstoneCounter extends StorableIterationCallback 
{
    private final TimeMaster timeMaster;
    private final long failAtTime;
    
    public int total = 0;
    public int tombstones = 0;

    public TombstoneCounter(TimeMaster timeMaster, long failAtTime) {
        this.timeMaster = timeMaster;
        this.failAtTime = failAtTime;
    }
    
    @Override public IterationAction verifyKey(StorableKey key) {
        if ((++total & 0x7F) == 0) {
            if (timeMaster.currentTimeMillis() >= failAtTime) {
                return IterationAction.TERMINATE_ITERATION;
            }
        }
        return IterationAction.PROCESS_ENTRY;
    }

    @Override
    public IterationAction processEntry(Storable entry) {
        if (entry.isDeleted()) {
            ++tombstones;
        }
        return IterationAction.PROCESS_ENTRY;
    }
    
}