package com.fasterxml.storemate.client.cluster;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.api.KeyRange;
import com.fasterxml.storemate.api.RequestPathBuilder;
import com.fasterxml.storemate.client.ServerNode;
import com.fasterxml.storemate.client.call.ContentDeleter;
import com.fasterxml.storemate.client.call.ContentGetter;
import com.fasterxml.storemate.client.call.ContentHeader;
import com.fasterxml.storemate.client.call.ContentPutter;

/**
 * Read-only part of state of a server node that is part of a cluster.
 */
public interface ServerNodeState
    extends ServerNode
{
    /*
    /**********************************************************************
    /* Basic state accessors
    /**********************************************************************
     */

    /**
     * Whether server node is disabled: usually occurs during shutdowns
     * and startups, and is considered a transient state. Clients typically
     * try to avoid GET access from disabled nodes; and schedule updates
     * (if any) after all enabled instances.
     */
    public boolean isDisabled();

    /*
    /**********************************************************************
    /* Key range access
    /**********************************************************************
     */
    
    public KeyRange getActiveRange();
    public KeyRange getPassiveRange();
    public KeyRange getTotalRange();

    /*
    /**********************************************************************
    /* Timestamp access
    /**********************************************************************
     */
    
    /**
     * Timestamp when last node state access request was sent.
     */
    public long getLastRequestSent();

    /**
     * Timestamp when last node state access response was received (note:
     * does NOT include cases where error occured during request).
     */
    public long getLastResponseReceived();

    /**
     * Timestamp when last node state response update was processed.
     */
    public long getLastNodeUpdateFetched();

    /**
     * Timestamp of the last update that has been fetched from the server node.
     */
    public long getLastClusterUpdateFetched();

    /**
     * Timestamp of the latest update for the server node.
     */
    public long getLastClusterUpdateAvailable();

    /*
    /**********************************************************************
    /* Call accessors, paths etc
    /**********************************************************************
     */
    
    /**
     * Accessor for finding URL for server endpoint used for
     * accessing (CRUD) of stored entries.
     */
    public <P extends RequestPathBuilder> P resourceEndpoint();

    public abstract <K extends EntryKey> ContentPutter<K> entryPutter();

    public abstract <K extends EntryKey> ContentGetter<K> entryGetter();

    public abstract <K extends EntryKey> ContentHeader<K> entryHeader();

    public abstract <K extends EntryKey> ContentDeleter<K> entryDeleter();
}
