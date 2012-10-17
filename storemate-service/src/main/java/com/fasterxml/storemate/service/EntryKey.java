package com.fasterxml.storemate.service;

import com.fasterxml.storemate.shared.StorableKey;

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
}
