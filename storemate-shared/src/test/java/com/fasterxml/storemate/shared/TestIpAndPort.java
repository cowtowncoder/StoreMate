package com.fasterxml.storemate.shared;

public class TestIpAndPort extends SharedTestBase
{
    public void testEquality()
    {
        final String DESC1 = "localhost:1234";
        IpAndPort first = new IpAndPort(DESC1);
        IpAndPort second = new IpAndPort("localhost:8080");

        assertEquals(first, first);
        assertEquals(second, second);
        assertFalse(first.equals(second));
        assertEquals(first, new IpAndPort(DESC1));
    }
}
