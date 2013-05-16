package com.fasterxml.storemate.store.backend;

/**
 * Base class for statistics exposed about store backends.
 * Since backends tend to expose quite
 * different stats, metrics, there is little in common
 * beyond simple type identifier (which may be used by caller).
 *<p>
 * Instances are most commonly either cast to expected type;
 * or serialized using a data-binding system like Jackson.
 * 
 * @since 0.9.8
 */
public abstract class BackendStats
{
    protected String _type;

    protected BackendStats() { }
    public BackendStats(String type) {
        _type = type;
    }
    
    /**
     * Accessor for basic type id that can be used to distinguish backend
     * types from each other.
     */
    public String getType() { return _type; }
}
