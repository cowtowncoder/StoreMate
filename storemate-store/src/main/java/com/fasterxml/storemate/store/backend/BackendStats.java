package com.fasterxml.storemate.store.backend;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base class for statistics exposed about store backends.
 * Since backends tend to expose quite
 * different stats, metrics, there is little in common
 * beyond simple type identifier (which may be used by caller).
 *<p>
 * Instances are most commonly either cast to expected type;
 * or serialized using a data-binding system like Jackson.
 */
public abstract class BackendStats
{
    protected String _type;

    /**
     * Timestamp of point when this instance was constructed;
     * useful for conservative estimate of freshness (or lack thereof)
     * for this data. Another way to think of it: the first possible timepoint
     * when these stats may have been collected.
     */
    protected long _creationTime;
    
    protected Boolean _fastStats;
    
    protected Long _timeTakenMsecs;

    protected BackendStats(BackendStats src) { 
        _type = src._type;
        _creationTime = src._creationTime;
        _fastStats = src._fastStats;
        _timeTakenMsecs = src._timeTakenMsecs;
    }

    public BackendStats(String type, long creationTime, BackendStatsConfig config)
    {
        _type = type;
        _fastStats = (config == null) ? null : config.onlyCollectFast();
        _creationTime = creationTime;
    }

    public void setTimeTakenMsecs(Long msecs) {
        _timeTakenMsecs = msecs;
    }

    /**
     * Accessor for basic type id that can be used to distinguish backend
     * types from each other.
     */
    public String getType() { return _type; }

    public Boolean getOnlyFastStats() { return _fastStats; }

    public long getCreationTime() { return _creationTime; }
    
    public Long getTimeTakenMsecs() { return _timeTakenMsecs; }

    /**
     * Accessor that can be used to get additional, backend-specific statistics
     * as a {@link java.util.Map}. These are typically also available via
     * explicit typed accessors, but this method can be used for generic access.
     */
    public Map<String,Object> extraStats() {
        return extraStats(new LinkedHashMap<String,Object>());
    }

    /**
     * Accessor that can be used to get additional, backend-specific statistics
     * as a {@link java.util.Map}. These are typically also available via
     * explicit typed accessors, but this method can be used for generic access.
     */
    public abstract Map<String,Object> extraStats(Map<String,Object> base);
}
