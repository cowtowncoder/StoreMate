package com.fasterxml.storemate.shared;

import java.io.IOException;

import org.junit.Assert;

public class TestUTF8Encoder extends SharedTestBase
{
    public void testSimple() throws Exception
    {
        _verify("Short and brutish");
        _verify("Scandic: \u00D8");
    }

    private void _verify(String str) throws IOException
    {
        byte[] jdk = str.getBytes("UTF-8");
        byte[] custom = UTF8Encoder.encodeAsUTF8(str);

        Assert.assertArrayEquals(jdk, custom);
    }
}
