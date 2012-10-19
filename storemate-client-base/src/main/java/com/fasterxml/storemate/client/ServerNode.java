package com.fasterxml.storemate.client;

import com.fasterxml.storemate.shared.IpAndPort;

public interface ServerNode
{
    /**
     * Address (protocol, host name, port number) of the server node
     */
    public IpAndPort getAddress();

}
