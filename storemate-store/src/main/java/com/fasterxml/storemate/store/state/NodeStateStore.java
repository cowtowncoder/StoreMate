package com.fasterxml.storemate.store.state;

import java.io.IOException;
import java.util.List;

import com.fasterxml.storemate.shared.util.RawEntryConverter;

/**
 * Base class that defines operations for storing and retrieving persistent
 * Node state information.
 * Store is used for persisting enough state regarding
 * peer-to-peer operations to reduce amount of re-synchronization
 * needed when node instances are restarted; and typically should use the
 * strongest possible persistence and consistency guarantees that the
 * backing data store implementation can offer: rate of operations should
 * be low.
 */
public abstract class NodeStateStore<K,V>
    implements com.fasterxml.storemate.shared.StartAndStoppable
{
    private final RawEntryConverter<K> _keyConverter;

    private final RawEntryConverter<V> _valueConverter;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    protected NodeStateStore(RawEntryConverter<K> keyConverter,
            RawEntryConverter<V> valueConverter)
    {
        if (keyConverter == null) {
            throw new IllegalArgumentException("Missing 'keyConverter'");
        }
        if (valueConverter == null) {
            throw new IllegalArgumentException("Missing 'valueConverter'");
        }
        _keyConverter = keyConverter;
        _valueConverter = valueConverter;
    }
    
    /*
    /**********************************************************************
    /* StartAndStoppable dummy implementation
    /**********************************************************************
     */

    @Override
    public void start() { }

    @Override
    public void prepareForStop() {
    }
    
    @Override
    public void stop() {
    }

    /*
    /**********************************************************************
    /* Public API: Content lookups
    /**********************************************************************
     */

    /**
     * Method that can be used to find specified entry, without updating
     * its last-accessed timestamp.
     */
    public final V findEntry(K key) throws IOException
    {
        return _findEntry(keyToRaw(key));
    }

    /**
     * Method for simply reading all node entries store has; called usually
     * only during bootstrapping.
     */
    public abstract List<V> readAll() throws IOException;

    /*
    /**********************************************************************
    /* Public API: Content modification
    /**********************************************************************
     */
    
    public final void upsertEntry(K key, V entry) throws IOException {
        _upsertEntry(keyToRaw(key), valueToRaw(entry));
    }

    public boolean deleteEntry(K key) throws IOException {
        return _deleteEntry(keyToRaw(key));
    }

    /*
    /**********************************************************************
    /* Abstract methods
    /**********************************************************************
     */

    protected abstract V _findEntry(byte[] rawKey) throws IOException;
    protected abstract void _upsertEntry(byte[] rawKey, byte[] rawValue) throws IOException;
    protected abstract boolean _deleteEntry(byte[] rawKey) throws IOException;

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected V rawToValue(byte[] raw, int offset, int length) throws IOException {
        return _valueConverter.fromRaw(raw, offset, length);
    }

    protected byte[] valueToRaw(V value) throws IOException {
        return _valueConverter.toRaw(value);
    }
    
    protected K rawToKey(byte[] raw, int offset, int length) throws IOException {
        return _keyConverter.fromRaw(raw, offset, length);
    }

    protected byte[] keyToRaw(K key) throws IOException {
        return _keyConverter.toRaw(key);
    }
}

