package com.fasterxml.storemate.shared;

/**
 * This is a container used to allow tests to "warp space/time continuum".
 * That is, during normal operation all it does is to dispatch operations
 * to the usual JDK methods; but when testing it can be diverted to
 * alter how time behaves, from perspective of both server and client side
 * code.
 *<p> 
 * Note on implementation: although it should often be adequate to just use
 * static set up (as most tests are linear), we unfortunately can not rely
 * on this: some tests suites explicitly use concurrent test execution.
 * As a result, we must use separate instances, which get carried throughout
 * the system.
 */
public abstract class TimeMaster
{
    /**
     * Factory method used for creating an instance that does not add any
     * test-time waiting logic.
     */
    public static TimeMaster nonTestInstance() {
        return new Std();
    }
    
    /*
    /**********************************************************************
    /* Basic instance API
    /**********************************************************************
     */

    /**
     * Method to call instead of {@link System#currentTimeMillis()} to obtain
     * current timestamp.
     */
    public abstract long currentTimeMillis();

    /**
     * Method to call instead of {@link Thread#sleep} for making current
     * thread sleep up to specified amount of time.
     */
    public abstract void sleep(long waitTime) throws InterruptedException;

    /**
     * Method that by-passes work-arounds and returns actual system time;
     * should be rarely needed, mostly for things that guard against
     * excessive resource usage.
     */
    public final long realSystemTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * @since 0.9.10
     */
    public long nanosForDiagnostics() {
        return System.nanoTime();
    }
    
    /*
    /**********************************************************************
    /* Shared helper methods
    /**********************************************************************
     */

    public static String timeDesc(long msecs)
    {
        if (msecs < 0L) {
            msecs = 0L;
        }
        if (msecs < 200L) { // let's switch to "0.x seconds" at 0.2...
            return String.format("%d msecs", msecs);
        }
        double secs = (double) msecs / 1000.0;
        if (secs < 60.0) {
            return String.format("%.1f secs", secs);
        }
        long minutes = ((long) secs) / 60L;
        if (minutes < 10L) {
            secs -= (60.0 * minutes);
            return String.format("%d mins, %d secs", minutes, (int) secs);
        }
        if (minutes < 60L) {
            return String.format("%d mins", minutes);
        }
        int hours = (int) (minutes / 60);
        if (hours < 48) {
            minutes -= (60 * hours);
            return String.format("%dh, %d mins", hours, minutes);
        }
        int days = hours / 24;
        if (days < 7) {
            hours -= (days * 24);
            return String.format("%dd, %d hours", hours, days);
        }
        return String.format("%d days", days);
    }

    /*
    /**********************************************************************
    /* Standard implementations
    /**********************************************************************
     */

    /**
     * Default implementation used when NOT running tests.
     */
    public static class Std extends TimeMaster
    {
        @Override
        public long currentTimeMillis() {
            return System.currentTimeMillis();
        }

        @Override
        public void sleep(long waitTime) throws InterruptedException {
            Thread.sleep(waitTime);
        }
    }
}
