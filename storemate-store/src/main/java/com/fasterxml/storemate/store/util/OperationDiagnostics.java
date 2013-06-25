package com.fasterxml.storemate.store.util;

import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.Storable;

/**
 * Helper class used for requesting and returning per-operation statistics
 * so that caller can update metrics and diagnostic information
 */
public class OperationDiagnostics
{
    /*
    /**********************************************************************
    /* Basic operation metadata
    /**********************************************************************
     */
    
    /**
     * Timestamp when diagnostics entry was created
     */
    protected final long _nanoStart;
    
    /*
    /**********************************************************************
    /* Item info
    /**********************************************************************
     */
    
    /**
     * Individual entry being added, accessed or removed, if applicable.
     */
    protected Storable _entry;

    /**
     * Number of items included in response, for operations where this
     * makes sense.
     */
    protected int _itemCount;
    
    /*
    /**********************************************************************
    /* Local DB access
    /**********************************************************************
     */

    /**
     * Accumulated timing information on primary database access calls.
     */
    protected TotalTime _dbAccess;

    /**
     * Accumulated timing information on wait time(s) for doing local
     * database access
     */
    protected TotalTime _dbWait;
    
    /*
    /**********************************************************************
    /* File system access
    /**********************************************************************
     */

    /**
     * Accumulated timing information on file system access.
     */
    protected TotalTime _fileAccess;

    /**
     * Accumulated timing information on wait time(s) for doing local
     * filesystem access
     */
    protected TotalTime _fileWait;
    
    /*
    /**********************************************************************
    /* Request/response handling
    /**********************************************************************
     */

    /**
     * Timestamp when content copy operation started (if any)
     */
    protected long _contentCopyStart;

    /**
     * Timestamp when content copy operation ended (if it did)
     */
    protected long _contentCopyEnd;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    public OperationDiagnostics(long nanoStart) {
        _nanoStart = nanoStart;
    }

    public OperationDiagnostics(TimeMaster tm) {
        this(tm.currentTimeMillis());
    }
    
    /*
    /**********************************************************************
    /* Item info
    /**********************************************************************
     */
    
    public OperationDiagnostics setEntry(Storable e) {
        _entry = e;
        return this;
    }

    public OperationDiagnostics setItemCount(int count) {
        _itemCount = count;
        return this;
    }

    /*
    /**********************************************************************
    /* DB Access info
    /**********************************************************************
     */

    public void addDbAccess(long nanos) {
        _dbAccess = TotalTime.createOrAdd(_dbAccess, nanos);
    }

    public void addDbWait(long nanos) {
        _dbWait = TotalTime.createOrAdd(_dbWait, nanos);
    }

    /*
    /**********************************************************************
    /* File system access
    /**********************************************************************
     */

    public void addFileAccess(long nanos) {
        _fileAccess = TotalTime.createOrAdd(_fileAccess, nanos);
    }

    public void addFileWait(long nanos) {
        _fileWait = TotalTime.createOrAdd(_fileWait, nanos);
    }
    
    /*
    /**********************************************************************
    /* Request/response handling
    /**********************************************************************
     */
    
    /**
     * Method called when content copy (between request and storage, or
     * storage and response) is being started.
     */
    public void startContentCopy(long startNanos) {
        _contentCopyStart = startNanos;
    }

    /**
     * Method called when content copy (between request and storage, or
     * storage and response) is being started.
     */
    public void startContentCopy() {
        startContentCopy(System.nanoTime());
    }
    
    public void finishContentCopy() {
        _contentCopyEnd = System.nanoTime();
    }
    
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */
    
    public Storable getEntry() { return _entry; }

    public int getItemCount() { return _itemCount; }
    
    /**
     * Accessor for number of nanoseconds spent since construction of this object
     */
    public long getNanosSpent() {
        return System.nanoTime() - _nanoStart;
    }

    public long getContentCopyNanos()
    {
        final long start = _contentCopyStart;
        if (start == 0L) {
            return 0L;
        }
        long end = _contentCopyEnd;
        if (end == 0L) {
            end = System.nanoTime();
        }
        return (end - start);
    }

    public boolean hasDbAccess() { return _dbAccess != null; }
    public boolean hasDbWait() { return _dbWait != null; }

    public boolean hasFileAccess() { return _fileAccess != null; }
    public boolean hasFileWait() { return _fileWait != null; }
    
    public boolean hasContentCopyNanos() {
        return (_contentCopyStart != 0L);
    }

    public TotalTime getDbAccess() { return _dbAccess; }
    public TotalTime getDbWait() { return _dbWait; }

    public TotalTime getFileAccess() { return _fileAccess; }
    public TotalTime getFileWait() { return _fileWait; }
}
