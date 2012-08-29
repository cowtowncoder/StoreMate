package com.fasterxml.storemate.shared;

import junit.framework.TestCase;

/**
 * Base class for unit tests
 */
public abstract class SharedTestBase extends TestCase
{
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