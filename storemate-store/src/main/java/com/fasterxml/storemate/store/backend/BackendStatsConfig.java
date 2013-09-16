package com.fasterxml.storemate.store.backend;

public class BackendStatsConfig
{
    /**
     * By default we only collect relatively inexpensive statistics
     */
    protected final static boolean DEFAULT_ONLY_FAST = true;
    
    /**
     * By default we do not reset statistics after collection
     */
    protected final static boolean DEFAULT_RESET = false;

    public final static BackendStatsConfig DEFAULT = new BackendStatsConfig();
    
    protected final boolean _fast;
    
    protected final boolean _resetAfterCollection;

    private BackendStatsConfig() {
        this(DEFAULT_ONLY_FAST, DEFAULT_RESET);
    }

    protected BackendStatsConfig(boolean fast, boolean reset)
    {
        _fast = fast;
        _resetAfterCollection = reset;
    }

    public BackendStatsConfig onlyCollectFast(boolean state) {
        return new BackendStatsConfig(state, _resetAfterCollection);
    }

    public BackendStatsConfig resetStatsAfterCollection(boolean state) {
        return new BackendStatsConfig(_fast, state);
    }
    
    public boolean onlyCollectFast() {
        return _fast;
    }

    public boolean resetStatsAfterCollection() {
        return _resetAfterCollection;
    }
}
