package com.fasterxml.storemate.client.cluster;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base class defined just to allow overriding logging of warning
 * messages; by default we'll just use JUL logging.
 */
public abstract class Loggable
{
    /**
     * Nasty hack to support tests, where we may want to force different
     * logging levels.
     */
    private static Level _globalLogLevel = null;
    
    /**
     * Class used as the id for loggers we access.
     */
    protected final Class<?> _loggingFor;

    /**
     * Locker object we use when lazily constructing Logger to use.
     */
    protected final Object _loggerLock = new Object();

    protected Logger stdLogger;

    protected Loggable() {
        _loggingFor = getClass();
    }
    
    protected Loggable(Class<?> loggingFor) {
        _loggingFor = loggingFor;
    }

    public boolean isInfoEnabled() {
        return _logger().isLoggable(Level.INFO);
    }
    
    public void logInfo(String msg)
    {
        _logger().log(Level.INFO, msg);
    }
    
    public void logWarn(String msg) {
        logWarn(null, msg);
    }
    
    public void logWarn(Throwable t, String msg)
    {
        // dynamic just because sub-classes probably don't need anything to do with JUL Logger:
        Logger logger = _logger();
        if (t == null) {
            logger.log(Level.WARNING, msg);
        } else {
            logger.log(Level.WARNING, msg, t);
        }
    }

    public void logError(String msg) {
        logError(null, msg);
    }
    
    public void logError(Throwable t, String msg)
    {
        Logger logger = _logger();
        if (t == null) {
            logger.log(Level.SEVERE, msg);
        } else {
            logger.log(Level.SEVERE, msg, t);
        }
    }

    protected Logger _logger()
    {
        synchronized (_loggerLock) {
            Logger logger = stdLogger;
            if (logger == null) {
                logger = Logger.getLogger(_loggingFor.getClass().getName());
                if (_globalLogLevel != null) {
                    logger.setLevel(_globalLogLevel);
                }
                stdLogger = logger;
            }
            return logger;
        }
    }

    public static void setTestLogLevel(Level l) {
        _globalLogLevel = l;
    }
}
