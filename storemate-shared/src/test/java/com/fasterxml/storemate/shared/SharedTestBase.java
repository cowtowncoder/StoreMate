package com.fasterxml.storemate.shared;

import java.util.zip.Adler32;

import junit.framework.TestCase;

/**
 * Base class for unit tests
 */
public abstract class SharedTestBase extends TestCase
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

    /*
    ///////////////////////////////////////////////////////////////////////
    // Checksum calculation
    ///////////////////////////////////////////////////////////////////////
     */

    protected int calcChecksum(byte[] data) 
    {
        Adler32 adler = new Adler32();
        adler.update(data);
        return (int) adler.getValue();
    }
}