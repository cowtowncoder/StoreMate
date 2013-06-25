package com.fasterxml.storemate.store.util;

/**
 * Simple helper class for tallying time spent on number of events
 * (calls, reads/writes).
 *<p>
 * Instances are not thread-safe and are to be used from a single
 * executing request thread; or, if passed along (for async processing),
 * need to be passed in synchronizing manner.
 */
public class TotalTime
{
    protected int _count;
    
    protected long _timeNanoSecs;
    
    public TotalTime(long nanoSecs) {
        _count = 1;
        _timeNanoSecs = nanoSecs;
    }

    public static TotalTime createOrAdd(TotalTime old, long nanoSecs)
    {
        if (old == null) {
            return new TotalTime(nanoSecs);
        }
        return old.add(nanoSecs);
    }
    
    public TotalTime add(long nanoSecs) {
        ++_count;
        _timeNanoSecs += nanoSecs;
        return this;
    }
    
    public int getCount() { return _count; }
    public long getTotalTime() { return _timeNanoSecs; }
}
