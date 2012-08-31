package com.fasterxml.storemate.store;

import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.storemate.shared.TimeMaster;

public class TimeMasterForSimpleTesting extends TimeMaster
{
    protected final AtomicLong _currentTime;

    /**
     * Timestamp at which point all currently blocked threads would be unblocked.
     */
    protected long _latestWaitTime;
    
    /**
     * We need a lock to ensure that handling of Threads blocked on
     * sleep() is atomically maintained.
     */
    protected final Object _blocked = new Object();

    public TimeMasterForSimpleTesting(long startTime)
    {
        _currentTime = new AtomicLong(startTime);
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Basic API
    ///////////////////////////////////////////////////////////////////////
     */
    
    @Override
    public long currentTimeMillis() {
        return _currentTime.get();
    }

    @Override
    public void sleep(long waitTime) throws InterruptedException
    {
        if (waitTime <= 0L) {
            return;
        }
        synchronized (_blocked) {
            final long end = _currentTime.get() + waitTime;
            if (end > _latestWaitTime) {
                _latestWaitTime = end;
            }
            
            do {
                _blocked.wait();
            } while (_currentTime.get() < end);
        }
        Thread.yield();
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Extended API for test code
    ///////////////////////////////////////////////////////////////////////
     */

    public long getMaxSleepTimeNeeded()
    {
        synchronized (_blocked) {
            long now = _currentTime.get();
            long diff = _latestWaitTime - now;
            return (diff <= 0L) ? 0L : diff;
        }
    }
    
    /**
     * Method for advancing the virtual time by specified number of milliseconds.
     */
    public void advanceCurrentTimeMillis(long delta)
    {
        if (delta < 0L) {
            throw new IllegalStateException("Can't move time back: illegal offset of "+delta+" msecs");
        }
        synchronized (_blocked) {
            _currentTime.addAndGet(delta);
            _blocked.notifyAll();
        }
    }

    /**
     * Method that can be used to advance virtual time enough to wake up
     * all threads that are sleeping for specified time periods
     * 
     * @return Number of milliseconds virtual time was advanced; may be 0
     *    if no advance was needed
     */
    public long advanceTimeToWakeAll()
    {
        long diff;
        synchronized (_blocked) {
            long now = _currentTime.get();
            // is there need to "wait"?
            diff = _latestWaitTime - now;
            if (diff <= 0L) {
                return 0L;
            }
            _currentTime.set(_latestWaitTime);
            _blocked.notifyAll();
        }
        return diff;
    }
    
    /**
     * Method for setting the virtual time to specified timestamp.
     * 
     * @param time Time to set: MUST not be less than current time
     */
    public void setCurrentTimeMillis(long time)
    {
        synchronized (_blocked) {
            long old = _currentTime.getAndSet(time);
            if (old > time) { // let's do a sanity check
                throw new IllegalStateException("Can't move time back from "+old+" to "+time);
            }
            _blocked.notifyAll();
        }
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Internal methods
    ///////////////////////////////////////////////////////////////////////
     */
}
