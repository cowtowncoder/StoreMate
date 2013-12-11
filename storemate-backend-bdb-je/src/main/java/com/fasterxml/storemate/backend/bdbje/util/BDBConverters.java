package com.fasterxml.storemate.backend.bdbje.util;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.util.WithBytesCallback;
import com.sleepycat.je.DatabaseEntry;

public class BDBConverters
{
    /**
     * Converter that converts whole key (or whatever source is)
     * into BDB-JE key.
     */
    public final static SimpleConverter simpleConverter = new SimpleConverter();

    public static class SimpleConverter
        implements WithBytesCallback<DatabaseEntry>
    {
        @Override
        public DatabaseEntry withBytes(byte[] buffer, int offset, int length) {
            if (offset == 0 && length == buffer.length) {
                return new DatabaseEntry(buffer);
            }
            return new DatabaseEntry(buffer, offset, length);
        }
    }

    public static DatabaseEntry dbKey(StorableKey rawKey) {
        return rawKey.with(simpleConverter);
    }
}
