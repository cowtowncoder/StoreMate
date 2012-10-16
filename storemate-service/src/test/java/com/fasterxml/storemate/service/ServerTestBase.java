package com.fasterxml.storemate.service;

import junit.framework.TestCase;

/**
 * Shared base class for unit tests; contains shared utility methods.
 */
public abstract class ServerTestBase extends TestCase
{
    /*
    ///////////////////////////////////////////////////////////////////////
    // Test methods: message validation
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
