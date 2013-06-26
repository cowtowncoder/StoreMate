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
     * Accumulated timing information on primary database access calls
     * excluding any wait time due to throttling.
     */
    protected TotalTime _dbAccess;

    /**
     * Accumulated timing information on primary database access calls
     * including any wait time due to throttling.
     */
    protected TotalTime _dbAccessTotal;
    
    /*
    /**********************************************************************
    /* File system access
    /**********************************************************************
     */

    /**
     * Accumulated timing information on file system access, not including possible
     * waits due to throttling.
     */
    protected TotalTime _fileAccess;

    /**
     * Accumulated timing information on wait time(s) for doing local
     * filesystem access including wait time due to throttling.
     */
    protected TotalTime _fileAccessTotal;
    
    /*
    /**********************************************************************
    /* Request/response handling
    /**********************************************************************
     */

    /**
     * Accumulated time for reading request data and/or writing response.
     */
    protected long _requestResponseTotal;
    
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

    public void addDbAccess(long nanoStart, long nanoDbStart, TimeMaster timeMaster) {
        addDbAccess(nanoStart, nanoDbStart, timeMaster.nanosForDiagnostics());
    }

    public void addDbAccess(long nanoStart, long nanoDbStart, long endTime) {
        final long rawTime = endTime - nanoDbStart;
        final long timeWithWait = endTime - nanoStart;
        _dbAccess = TotalTime.createOrAdd(_dbAccess, rawTime, timeWithWait);
    }

    /*
    /**********************************************************************
    /* File system access
    /**********************************************************************
     */

    public void addFileAccess(long nanoStart, long nanoFileStart, TimeMaster timeMaster) {
        addFileAccess(nanoStart, nanoFileStart, timeMaster.nanosForDiagnostics());
    }
    
    public void addFileAccess(long nanoStart, long nanoFileStart, long endTime) {
        final long rawTime = endTime - nanoFileStart;
        final long timeWithWait = endTime - nanoStart;
        _fileAccess = TotalTime.createOrAdd(_fileAccess, rawTime, timeWithWait);
    }

    public void addFileWait(long waitTime) {
        _fileAccess = TotalTime.createOrAdd(_fileAccess, 0L, waitTime);
    }
    
    /*
    /**********************************************************************
    /* Request/response handling
    /**********************************************************************
     */

    public void addRequestReadTime(long nanoStart, TimeMaster tm) {
        addRequestReadTime(nanoStart, tm.nanosForDiagnostics());
    }
    
    public void addRequestReadTime(long nanoStart, long nanoEnd) {
        _requestResponseTotal += (nanoEnd - nanoStart);
    }

    public void addResponseWriteTime(long nanoStart, TimeMaster tm) {
        addResponseWriteTime(nanoStart, tm.nanosForDiagnostics());
    }
    
    public void addResponseWriteTime(long nanoStart, long nanoEnd) {
        addResponseWriteTime(nanoEnd - nanoStart);
    }

    public void addResponseWriteTime(long nanoSecs) {
        _requestResponseTotal += nanoSecs;
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

    public boolean hasDbAccess() { return _dbAccess != null; }
    public boolean hasFileAccess() { return _fileAccess != null; }

    public boolean hasRequestResponseTotal() {
        return (_requestResponseTotal > 0L);
    }

    public long getRequestResponseTotal() {
        return _requestResponseTotal;
    }

    public TotalTime getDbAccess() { return _dbAccess; }

    public TotalTime getFileAccess() { return _fileAccess; }
}
