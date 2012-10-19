package com.fasterxml.storemate.client;

import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Minimal information about a server node; information that must be
 * accessible for per-call (single-node) requests and responses.
 */
public interface ServerNode
{
    /**
     * Address (protocol, host name, port number) of the server node
     */
    public IpAndPort getAddress();

}
