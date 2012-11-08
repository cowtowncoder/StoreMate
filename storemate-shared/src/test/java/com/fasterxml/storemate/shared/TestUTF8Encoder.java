package com.fasterxml.storemate.shared;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;

import com.fasterxml.storemate.shared.util.UTF8Encoder;

public class TestUTF8Encoder extends SharedTestBase
{
    private final static String STRING1 = "Short and brutish";
    private final static String STRING2 = "Scandic: \u00D8";
    
    /*
    /**********************************************************************
    /* Encoding tests
    /**********************************************************************
     */
    
    public void testSimpleEncoding() throws Exception
    {
        _verify(STRING1);
        _verify(STRING2);
    }

    public void testPrefixEncoding() throws Exception
    {
        final byte[] PREFIX = new byte[] { 1, 2, 3};
        _verifyPrefix(STRING1, PREFIX);
        _verifyPrefix(STRING2, PREFIX);
    }

    public void testEncodingWithFluff() throws Exception
    {
        _verifyFluff(STRING1, 5, 30);
        _verifyFluff(STRING2, 0, 27);
        _verifyFluff(STRING2, 39, 0);

        _verifyFluff(STRING1, 69000, 1);
        _verifyFluff(STRING2, 13, 73500);
    }

    /*
    /**********************************************************************
    /* Secondary test methods
    /**********************************************************************
     */
    
    private void _verify(String str) throws IOException
    {
        byte[] jdk = str.getBytes("UTF-8");
        byte[] custom = UTF8Encoder.encodeAsUTF8(str);

        Assert.assertArrayEquals(jdk, custom);
    }

    private void _verifyPrefix(String str, byte[] prefix) throws IOException
    {
        byte[] jdk = str.getBytes("UTF-8");
        byte[] custom = UTF8Encoder.encodeAsUTF8(prefix, str);
        byte[] exp = Arrays.copyOf(prefix, prefix.length + jdk.length);
        System.arraycopy(jdk, 0, exp, prefix.length, jdk.length);

        Assert.assertArrayEquals(exp, custom);
    }

    private void _verifyFluff(String str, int prefix, int suffix) throws IOException
    {
        byte[] jdk = str.getBytes("UTF-8");
        byte[] custom = UTF8Encoder.encodeAsUTF8(str, prefix, suffix, true);
        byte[] exp = new byte[jdk.length + prefix + suffix];
        System.arraycopy(jdk, 0, exp, prefix, jdk.length);

        Assert.assertArrayEquals(exp, custom);
    }
}
