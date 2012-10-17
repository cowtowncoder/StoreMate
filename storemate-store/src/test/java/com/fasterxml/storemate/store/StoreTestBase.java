package com.fasterxml.storemate.store;

import com.fasterxml.storemate.shared.StorableKey;

import com.fasterxml.storemate.shared.SharedTestBase;
import com.fasterxml.storemate.shared.util.UTF8Encoder;

/**
 * Base class for unit tests of server sub-module
 */
public abstract class StoreTestBase extends SharedTestBase
{
    public StorableKey storableKey(String str)
    {
        return new StorableKey(UTF8Encoder.encodeAsUTF8(str));
    }

}