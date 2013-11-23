package com.fasterxml.storemate.store.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

/**
 * Helper class used for doing throttled (at most one message of type
 * within given time period) logging. Note that suppression is purely
 * based on time period; and not on level or content of messages. That is,
 * folding is blind and simply focused on limiting amount of output,
 * even if at expense of hiding different output types.
 * Assumption there is, then, that messages tend to be either identical, or
 * that exact details are less important than existence of logging.
 */
public class SimpleLogThrottler
{
    private final Logger _logger;

    private final AtomicLong suppressUntil = new AtomicLong(0L);

    private final AtomicInteger suppressCount = new AtomicInteger(0);
    
    /**
     * Delay (in msecs) used for throttling
     */
    private final int delayBetweenMsecs;

    public SimpleLogThrottler(Logger logger, int msecsToThrottle)
    {
        _logger = logger;
        delayBetweenMsecs = msecsToThrottle;
    }

    /*
    /**********************************************************************
    /* Public API
    /**********************************************************************
     */
    
    public boolean logWarn(String msg, Object... arguments) {
        return logWarn(System.currentTimeMillis(), msg, arguments);
    }

    public boolean logWarn(long timeNow, String msg, Object... arguments)
    {
        Integer supp = canProceed(timeNow);
        if (supp == null) {
            return false;
        }
        if (supp.intValue() > 0) {
            _logger.warn("(suppressed {}) " + msg, supp, arguments);
        } else {
            _logger.warn(msg, arguments);
        }
        return true;
    }

    public boolean logError(String msg, Object... arguments) {
        return logError(System.currentTimeMillis(), msg, arguments);
    }

    public boolean logError(long timeNow, String msg, Object... arguments)
    {
        Integer supp = canProceed(timeNow);
        if (supp == null) {
            return false;
        }
        if (supp.intValue() > 0) {
            _logger.error("(suppressed {}) " + msg, supp, arguments);
        } else {
            _logger.error(msg, arguments);
        }
        return true;
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    /**
     * Method to call to try to do whatever operation is being throttled;
     * if null is returned, operation should block.
     * Otherwise, number of previous throttled calls is returned, and counters
     * reset
     */
    protected Integer canProceed(long timeNow)
    {
        long until = suppressUntil.get();

        // Either in throttle; or we lose the race
        if ((timeNow < until) 
                 || !suppressUntil.compareAndSet(until, timeNow+delayBetweenMsecs)) {
            // either way, consider this a suppressed call:
            suppressCount.addAndGet(1);
            return null;
        }
        // otherwise we got it; see what had been suppressed
        int suppressed = suppressCount.getAndSet(0);
        return Integer.valueOf(suppressed);
    }

    // // // Test support

    protected long willSuppressUntil() {
        return suppressUntil.get();
    }

    protected int suppressCount() {
        return suppressCount.get();
    }
}
