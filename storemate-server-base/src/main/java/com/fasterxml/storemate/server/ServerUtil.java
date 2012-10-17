package com.fasterxml.storemate.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collection;
import java.util.Enumeration;

public class ServerUtil
{
    public static void findLocalIPs(Collection<InetAddress> ips) throws IOException
    {
        Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
        while (en.hasMoreElements()) {
            NetworkInterface networkInt = en.nextElement();
            Enumeration<InetAddress> en2 = networkInt.getInetAddresses();
            while (en2.hasMoreElements()) {
                ips.add(en2.nextElement());
            }
        }
    }
}
