package com.fasterxml.storemate.client;


/**
 * Container class for an ordered set of references to server nodes
 * that should be contacted for accessing content for given
 * key, based on key hash based matching.
 */
public class NodesForKey
{
    private final static ServerNodeState[] NO_NODES = new ServerNodeState[0];
    
    private final int _version;

    private final ServerNodeState[] _nodes;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */
    
    public NodesForKey(int version, ServerNodeState[] nodes)
    {
        _version = version;
        _nodes = nodes;
    }

    public static NodesForKey empty(int version)
    {
        return new NodesForKey(version, NO_NODES);
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Accessors
    ///////////////////////////////////////////////////////////////////////
     */
    
    public int version() { return _version; }

    public int size() { return _nodes.length; }
    
    public ServerNodeState node(int index) {
        return _nodes[index];
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Overrides
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[Nodes (").append(_nodes.length).append(") ");
        for (int i = 0, end = _nodes.length; i < end; ++i) {
            if (i > 0) {
                sb.append(", ");
            }
            ServerNodeState node = _nodes[i];
            sb.append(node.getAddress()).append(": ranges=");
            sb.append(node.getActiveRange()).append('/').append(node.getPassiveRange());
        }
        sb.append(")");
        return sb.toString();
    }
}
