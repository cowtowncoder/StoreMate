package com.fasterxml.storemate.store.util;

/**
 * Extension of {@link TotalTime} that also tracks cumulative
 * byte counts.
 */
public class TotalTimeAndBytes extends TotalTime
{
    protected long _bytes;
    
    public TotalTimeAndBytes(long nanoSecsRaw, long nanoSecsWithWait,
            long bytes)
    {
        super(nanoSecsRaw, nanoSecsWithWait);
        _bytes = bytes;
    }

    public static TotalTimeAndBytes createOrAdd(TotalTimeAndBytes old,
            long nanoSecsRaw, long nanoSecsWithWait, long bytes)
    {
        if (old == null) {
            return new TotalTimeAndBytes(nanoSecsRaw, nanoSecsWithWait, bytes);
        }
        return old.add(nanoSecsRaw, nanoSecsWithWait, bytes);
    }

    public TotalTimeAndBytes add(long nanoSecsRaw, long nanoSecsWithWait, long bytes) {
        _bytes += bytes;
        add(nanoSecsRaw, nanoSecsWithWait);
        return this;
    }

    public long getBytes() {
        return _bytes;
    }
}
