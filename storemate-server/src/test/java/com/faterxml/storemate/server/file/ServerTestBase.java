package com.faterxml.storemate.server.file;

import junit.framework.TestCase;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.UTF8Encoder;

/**
 * Base class for unit tests of server sub-module
 */
public abstract class ServerTestBase extends TestCase
{
    /*
    ///////////////////////////////////////////////////////////////////////
    // Test methods: factory methods
    ///////////////////////////////////////////////////////////////////////
     */

	public StorableKey storableKey(String str)
	{
		return new StorableKey(UTF8Encoder.encodeAsUTF8(str));
	}

    /*
    ///////////////////////////////////////////////////////////////////////
    // Test methods: exception verification
    ///////////////////////////////////////////////////////////////////////
     */

    protected void verifyException(Exception e, String expected)
    {
        verifyMessage(expected, e.getMessage());
    }
    
    protected void verifyMessage(String expectedPiece, String actual)
    {
        if (actual == null || actual.toLowerCase().indexOf(expectedPiece.toLowerCase()) < 0) {
            fail("Expected message that contains phrase '"+expectedPiece+"'; instead got: '"
                    +actual+"'");
        }
    }
}