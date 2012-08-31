package com.fasterxml.storemate.store;

import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * General-purpose exception used instead of basic {@link IOException},
 * to allow catching of exceptions produced by store stuff, as opposed
 * to underlying I/O system.
 */
public class StoreException extends IOException
{
    private static final long serialVersionUID = 1L;

    protected final StorableKey _key;
    
    public StoreException(StorableKey key, String msg) {
        super(msg);
        _key = key;
    }

    public StoreException(StorableKey key, Throwable t) {
        super(t);
        _key = key;
    }

    public StoreException(StorableKey key, String msg, Throwable t) {
        super(msg, t);
        _key = key;
    }

    /**
     * Method for accessing {@link StorableKey} for entry being operated on
     * (if any) when this Exception occurred.
     */
    public StorableKey getKey() {
        return _key;
    }
}
