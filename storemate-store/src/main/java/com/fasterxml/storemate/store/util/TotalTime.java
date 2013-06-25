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

    /**
     * Timing that only includes operation itself but not any additional
     * wait time.
     */
    protected long _timeNanoSecs;

    /**
     * Alternate count that includes possible wait time(s).
     */
    protected long _timeNanoSecsTotal;
    
    public TotalTime(long nanoSecsRaw, long nanoSecsWithWait)
    {
        _count = 1;
        _timeNanoSecs = nanoSecsRaw;
        _timeNanoSecsTotal = nanoSecsWithWait;
    }

    public static TotalTime createOrAdd(TotalTime old,
            long nanoSecsRaw, long nanoSecsWithWait)
    {
        if (old == null) {
            return new TotalTime(nanoSecsRaw, nanoSecsWithWait);
        }
        return old.add(nanoSecsRaw, nanoSecsWithWait);
    }

    public TotalTime add(long nanoSecsRaw, long nanoSecsWithWait) {
        ++_count;
        _timeNanoSecs += nanoSecsRaw;
        _timeNanoSecsTotal += nanoSecsWithWait;
        return this;
    }
    
    public int getCount() { return _count; }

    public long getTotalTimeWithoutWait() { return _timeNanoSecs; }
    public long getTotalTimeWithWait() { return _timeNanoSecsTotal; }
}
