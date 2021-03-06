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
    
    /*
    /**********************************************************************
    /* File system access
    /**********************************************************************
     */

    /**
     * Accumulated timing information on file system access, not including possible
     * waits due to throttling.
     */
    protected TotalTimeAndBytes _fileAccess;

    protected boolean _hasFileReads;

    protected boolean _hasFileWrites;
    
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
        this(tm.nanosForDiagnostics());
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
    /* File system access, reads
    /**********************************************************************
     */

    public void addFileReadAccess(long nanoStart, TimeMaster timeMaster, long bytes) {
        addFileReadAccess(nanoStart, nanoStart, timeMaster.nanosForDiagnostics(), bytes);
    }
    
    public void addFileReadAccess(long nanoStart, long nanoFileStart, TimeMaster timeMaster, long bytes) {
        addFileReadAccess(nanoStart, nanoFileStart, timeMaster.nanosForDiagnostics(), bytes);
    }
    
    public void addFileReadAccess(long nanoStart, long nanoFileStart, long endTime, long bytes) {
        _hasFileReads = true;
        final long rawTime = endTime - nanoFileStart;
        final long timeWithWait = endTime - nanoStart;
        _fileAccess = TotalTimeAndBytes.createOrAdd(_fileAccess, rawTime, timeWithWait, bytes);
    }

    public void addFileReadWait(long waitTime) {
        _hasFileReads = true;
        _fileAccess = TotalTimeAndBytes.createOrAdd(_fileAccess, 0L, waitTime, 0L);
    }

    /*
    /**********************************************************************
    /* File system access, writes
    /**********************************************************************
     */
    
    public void addFileWriteAccess(long nanoStart, TimeMaster timeMaster, long bytes) {
        addFileWriteAccess(nanoStart, nanoStart, timeMaster.nanosForDiagnostics(), bytes);
    }
    
    public void addFileWriteAccess(long nanoStart, long nanoFileStart, TimeMaster timeMaster,
            long bytes) {
        addFileWriteAccess(nanoStart, nanoFileStart, timeMaster.nanosForDiagnostics(), bytes);
    }
    
    public void addFileWriteAccess(long nanoStart, long nanoFileStart, long endTime, long bytes) {
        _hasFileWrites = true;
        final long rawTime = endTime - nanoFileStart;
        final long timeWithWait = endTime - nanoStart;
        _fileAccess = TotalTimeAndBytes.createOrAdd(_fileAccess, rawTime, timeWithWait, bytes);
    }

    public void addFileWriteWait(long waitTime) {
        _hasFileWrites = true;
        _fileAccess = TotalTimeAndBytes.createOrAdd(_fileAccess, 0L, waitTime, 0L);
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

    public boolean hasFileReads() { return _hasFileReads; }
    public boolean hasFileWrites() { return _hasFileWrites; }
    
    public boolean hasRequestResponseTotal() {
        return (_requestResponseTotal > 0L);
    }

    public long getRequestResponseTotal() {
        return _requestResponseTotal;
    }

    public TotalTime getDbAccess() { return _dbAccess; }

    public TotalTimeAndBytes getFileAccess() { return _fileAccess; }
}
