package com.fasterxml.storemate.shared;

/**
 * Simple add-on interface, to denote things are to be managed according
 * to life-cycle of container. This is typically implemented by service
 * components, but may also be used by client-side components when there
 * is costly state to initialize and maintain.
 */
public interface StartAndStoppable
{
    /**
     * Method called when the component should start its operation.
     * This occurs after initialization (if any; and out of scope for
     * this interface)
     */
    public void start() throws java.lang.Exception;

    /**
     * Method called before {@link #stop}, and is meant to start graceful
     * shutdown.
     */
    public void prepareForStop() throws Exception;

    /**
     * Method called when the component is to shut down; follows call to
     * {@link #prepareForStop()}, possibly with some delay.
     */
    public void stop() throws java.lang.Exception;
}
