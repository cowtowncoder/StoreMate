package com.fasterxml.storemate.store.lastaccess;

/**
 * Interface that defines abstraction used for storing optional information
 * about last-access time for entries, for purpose of defining life-time
 * of an entry in terms of last-access, instead of creation time.
 *<p>
 * Although few details are defined here, one constraint is that number of
 * different methods must fit in a byte; so assumption is that only a relatively
 * small number of methods are defined by implementations.
 */
public interface LastAccessUpdateMethod
{
    public int asInt();
    public byte asByte();

    /**
     * Accessor for determining whether given implementation value means
     * "do not use or update last-access information"; usually one of
     * enumerated values does this.
     */
    public boolean meansNoUpdate();
}
