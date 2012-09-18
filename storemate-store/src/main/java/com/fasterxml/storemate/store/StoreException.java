package com.fasterxml.storemate.store;

import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * General-purpose exception used instead of basic {@link IOException},
 * to allow catching of exceptions produced by store stuff, as opposed
 * to underlying I/O system.
 */
public abstract class StoreException extends IOException
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

    public abstract boolean isInputError();

    public abstract boolean isServerError();
    
    /*
    /**********************************************************************
    /* Sub-types
    /**********************************************************************
     */

    /**
     * Specific {@link StoreException} subtype used when the problem is
     * with input data passed to an operation. This will typically
     * result in a different kind of external error being reported.
     */
    public static class Input extends StoreException
    {
        private static final long serialVersionUID = 1L;

        public Input(StorableKey key, String msg) {
            super(key, msg);
        }

        public Input(StorableKey key, Throwable t) {
            super(key, t);
        }

        public Input(StorableKey key, String msg, Throwable t) {
            super(key, msg, t);
        }

        @Override public boolean isInputError() { return true; }
        @Override public boolean isServerError() { return false; }
    }
    
    /**
     * Specific {@link StoreException} subtype used for problems that are
     * due to state of store itself, and not caused by caller's actions.
     */
    public static class Internal extends StoreException
    {
        private static final long serialVersionUID = 1L;

        public Internal(StorableKey key, String msg) {
            super(key, msg);
        }

        public Internal(StorableKey key, Throwable t) {
            super(key, t);
        }

        public Internal(StorableKey key, String msg, Throwable t) {
            super(key, msg, t);
        }

        @Override public boolean isInputError() { return false; }
        @Override public boolean isServerError() { return true; }
    }

    /**
     * Specific {@link StoreException} subtype used for problems
     * that are due I/O operations, but can not be exposed as
     * {@link IOException}s for some reason.
     */
    public static class IO extends StoreException
    {
        private static final long serialVersionUID = 1L;

        public IO(StorableKey key, IOException t) {
            super(key, t);
        }

        public IO(StorableKey key, String msg, IOException t) {
            super(key, msg, t);
        }

        @Override public boolean isInputError() { return false; }
        @Override public boolean isServerError() { return true; }
    }
}
