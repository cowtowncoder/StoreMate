package com.fasterxml.storemate.shared;

import com.fasterxml.storemate.shared.util.UTF8UrlEncoder;

public class TestUTF8UrlEncoder extends SharedTestBase
{

    /*
    /**********************************************************************
    /* Encoding tests
    /**********************************************************************
     */

    public void testEncoding()
    {
        UTF8UrlEncoder enc = new UTF8UrlEncoder(true);
        assertEquals("This%3A+%C2%A9...", enc.encode("This: \u00A9...", false));
        assertEquals("here/then", enc.encode("here/then", false));
        assertEquals("here%2Fthen", enc.encode("here/then", true));
    }

    /*
    /**********************************************************************
    /* Decoding tests
    /**********************************************************************
     */

    public void testDecoding()
    {
        UTF8UrlEncoder enc = new UTF8UrlEncoder(true);

        // slash handling
        assertEquals("here/then", enc.decode("here/then"));
        assertEquals("here/then", enc.decode("here%2Fthen"));
        
        // non-ASCII:
        assertEquals("This: \u00A9...", enc.decode("This%3A+%C2%A9..."));
    }
}
