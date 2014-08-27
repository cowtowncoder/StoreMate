package com.fasterxml.storemate.store.backend;

import java.io.File;

import com.fasterxml.storemate.shared.util.RawEntryConverter;
import com.fasterxml.storemate.store.StoreConfig;
import com.fasterxml.storemate.store.state.NodeStateStore;

/**
 * Base class for builder objects used for constructing
 * {@link StoreBackend} instances.
 *
 * @param <T> Type of configuration object builder takes
 */
public abstract class StoreBackendBuilder<T extends StoreBackendConfig>
{
    protected final Class<T> _configType;

    protected StoreBackendBuilder(Class<T> configType) {
        _configType = configType;
    }
    
    /**
     * Method that framework can use to figure out type of configuration
     * object, to be able to data-bind it from external configuration
     * (such as JSON file)
     */
    public Class<T> getConfigClass() { return _configType; }
    
    /**
     * Fluent factory method used for (re)configuring builder with
     * given generic Store configuration.
     */
    public abstract StoreBackendBuilder<?> with(StoreConfig genericConfig);

    /**
     * Fluent factory method used for (re)configuring builder with
     * given configuration.
     */
    public abstract StoreBackendBuilder<?> with(StoreBackendConfig backendConfig);

    /**
     * Factory method called after all configuration has been passed, to create
     * the store instance.
     */
    public abstract StoreBackend build();

    /**
     * Factory method called after all configuration has been passed, to create
     * the store used for persisting node state information.
     */
    public abstract <K,V> NodeStateStore<K,V> buildNodeStateStore(File metadataRoot,
            RawEntryConverter<K> keyConv, RawEntryConverter<V> valueConv);

    /**
     * Factory method that may be called to create a secondary node-state store,
     * typically for storing remote-cluster information.
     * 
     * @since 1.1.2
     */
    public abstract <K,V> NodeStateStore<K,V> buildSecondaryNodeStateStore(File metadataRoot,
            String secondaryId,
            RawEntryConverter<K> keyConv, RawEntryConverter<V> valueConv);


    protected File _concatAndCreate(File root, String path)
    {
        File dir = root;
        for (String part : path.split("/")) {
            part = part.trim();
            if (!part.isEmpty()) { // to remove possible duplicate slashes, avoid empty segments
                dir = new File(dir, part);
            }
        }
        if (!dir.exists() || !dir.isDirectory()) {
            if (!dir.mkdirs()) {
                throw new IllegalArgumentException("Directory '"+dir.getAbsolutePath()+"' did not exist: failed to create it");
            }
        }
        return dir;
    }
}
