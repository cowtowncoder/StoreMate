package com.fasterxml.storemate.shared;

import java.net.InetAddress;

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

    public void testParsing()
    {
        final String IP_STR = "https://google.com:8080";
        IpAndPort ip = new IpAndPort(IP_STR);
        
        assertEquals("https", ip.getProcol());
        assertEquals("google.com", ip.getHostName());
        assertEquals(8080, ip.getPort());
        // note: end point adds trailing slash
        assertEquals(IP_STR+"/", ip.toString());
    }
    
    public void testResolution() throws Exception
    {
        // for fun, add trailing slash
        IpAndPort ip = new IpAndPort("http://google.com:80/");
        InetAddress addr = ip.getIP();
        assertEquals("google.com", addr.getHostName());
    }
}
