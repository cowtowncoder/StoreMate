package com.fasterxml.storemate.server.bdb;

import java.util.Arrays;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

/**
 * Helper class we need for creating keys for the last-mod-time index.
 * This is simple since first 8 bytes of the primary entry are the
 * timestamp needed.
 */
public class LastModKeyCreator implements SecondaryKeyCreator
{
    @Override
    public boolean createSecondaryKey(SecondaryDatabase secondary,
            DatabaseEntry key, DatabaseEntry data, DatabaseEntry result)
    {
        // sanity check first:
        byte[] raw = data.getData();
        if (raw.length < 8) {
            throw new IllegalStateException("Illegal entry, with length of "+raw.length
                    +" (less than 8 bytes): can not create secondary key");
        }
        // not sure if BDB-JE shares/reuses data, let's not take chances:
        byte[] rawKey = Arrays.copyOf(raw, 8);
        result.setData(rawKey);
        return true;
    }
}
