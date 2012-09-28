package com.fasterxml.storemate.shared;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Simple value type used for basic "IP:PORT" style Strings extracted
 * from configuration file.
 */
public final class IpAndPort
{
    protected final String _ipName;

    protected final int _port;

    protected final int _hashCode;
    
    // lazily resolved actual address
    protected volatile InetAddress _ipAddress;

    // lazily constructed end point 
    protected volatile String _endpoint;
    
    // note: could add @JsonCreator, but not required:
    public IpAndPort(String str)
    {
        if (str == null) {
            str = "";
        }
        int ix;

        if ((ix = str.indexOf(':')) < 0) {
            throw new IllegalArgumentException("Can not decode IP address and port number from String '"
                    +str+"'");
        }
        String portStr = str.substring(ix+1).trim();
        try {
            _port = Integer.parseInt(portStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid port number '"+portStr+"' (String '"+str+"'); not a valid int");
        }
        if (_port < 0 || _port >= 0x10000) {
            throw new IllegalArgumentException("Invalid port number ("+_port+"): (String '"+str+"') must be [0, 65535]");
        }
        // can assume, for now, that IP part is ok, as long as it's not empty
        _ipName = str.substring(0, ix).trim();
        if (_ipName.isEmpty()) {
            throw new IllegalArgumentException("Missing IP name (String '"+str+"')");
        }
        _hashCode = _ipName.hashCode() ^ _port;
    }

    public IpAndPort(String ipName, int port)
    {
        _ipName = ipName;
        _port = port;
        _hashCode = _ipName.hashCode() ^ _port;
    }
    
    public IpAndPort withPort(int port)
    {
        if (_port == port) {
            return this;
        }
        return new IpAndPort(_ipName, port);
    }
    
    @Override
    public boolean equals(Object o)
    {
        if (o == this) return true;
        if (o == null) return false;
        if (o.getClass() != getClass()) return false;
        IpAndPort other = (IpAndPort) o;
        return (other._port == _port) && _ipName.equals(other._ipName);
    }
    
    @Override
    public int hashCode() { return _hashCode; }

    @Override
    public String toString() {
        return _ipName + ":" + _port;
    }
	
    public int getPort() { return _port; }

    /**
     * Simple accessor for checking whether this endpoint refers to
     * local host using either "localhost" or "127.0.0.1" references;
     * note, though, that it does not check to see if it might be
     * referring to local instance via real IP number.
     */
    public boolean isLocalReference() {
        return "localhost".equals(_ipName) || "127.0.0.1".equals(_ipName);
    }
    
    /**
     * Accessor for getting URL end point like "http://somehost.com:8080/".
     * Note that no address resolution is performed; this to allow dynamic
     * resolution depending on DNS-caching settings.
     */
    public String getEndpoint()
    {
        String str = _endpoint;
        if (str == null) {
            str = "http://"+_ipName+":"+_port+"/";
            _endpoint = str;
        }
        return str;
    }

    public String getEndpoint(String path)
    {
        String base = getEndpoint();
        StringBuilder sb = new StringBuilder(base.length() + path.length());
        sb.append(base);
        sb.append(path);
        return sb.toString();
    }
    
    public InetAddress getIP() throws UnknownHostException
    {
        InetAddress addr = _ipAddress;
        if (addr == null) {
            _ipAddress = addr = InetAddress.getByName(_ipName);
        }
        return addr;
    }
}
