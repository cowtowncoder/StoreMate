package com.fasterxml.storemate.store.backend;

import com.fasterxml.storemate.store.StoreConfig;

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
     * Method called after all configuration has been passed, to create
     * the store instance.
     */
    public abstract StoreBackend build();
}
