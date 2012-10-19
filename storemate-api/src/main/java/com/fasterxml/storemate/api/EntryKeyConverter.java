package com.fasterxml.storemate.api;

import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.hash.IncrementalHasher32;

/**
 * Abstract class that defines interface to use for converting
 * "raw" {@link StorableKey} instances into higher level key
 * abstractions, constructing such keys, and calculating
 * routing hash codes.
 */
public abstract class EntryKeyConverter<K extends EntryKey>
{
    /**
     * Method called to reconstruct a {@link VKey} from raw bytes.
     */
    public abstract K construct(byte[] rawKey);

    /**
     * Method called to reconstruct a {@link VKey} from raw bytes.
     */
    public abstract K construct(byte[] rawKey, int offset, int length);
    
    /**
     * Method called to construct a "refined" key out of raw
     * {@link StorableKey}
     */
    public abstract K rawToEntryKey(StorableKey key);

    /**
     * Method called to figure out raw hash code to use for routing request
     * regarding given content key.
     */
    public abstract int routingHashFor(K key);

    public abstract int contentHashFor(ByteContainer bytes);

    /**
     * Method that will create a <b>new</b> hasher instance for calculating
     * hash values for content that can not be handled as a single block.
     */
    public abstract IncrementalHasher32 createStreamingContentHasher();
}
