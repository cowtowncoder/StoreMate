package com.fasterxml.storemate.backend.leveldb;

import java.io.IOException;

import org.iq80.leveldb.DBException;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.StoreException;

public class LevelDBUtil
{
    public static <T> T convertDBE(StorableKey key, DBException dbException)
        throws StoreException
    {
        // any special types that require special handling... ?
        /*
        if (dbException instanceof LockTimeoutException) {
            throw new StoreException.ServerTimeout(key, dbException);
        }
        */
        throw new StoreException.DB(key, StoreException.DBProblem.OTHER, dbException);
    }

    public static <T> T convertIOE(StorableKey key, IOException ioe)
        throws StoreException
    {
        // any special types that require special handling... ?
        throw new StoreException.Internal(key, ioe);
    }
}
