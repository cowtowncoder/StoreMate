package com.fasterxml.storemate.shared;


/**
 * Base class for application-specific "refined" keys, constructed usually
 * from {@link StorableKey}, but sometimes 
 *<p>
 * Instances must be immutable and usable as {@link java.util.Map} keys.
 */
public abstract class EntryKey
{
    public abstract StorableKey asStorableKey();

    public abstract byte[] asBytes();

    /**
     * Method for appending key information into path, using given path builder.
     */
    public abstract <B extends RequestPathBuilder> B appendToPath(B pathBuilder);
}
