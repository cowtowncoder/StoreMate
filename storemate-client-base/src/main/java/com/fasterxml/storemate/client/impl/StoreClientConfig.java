package com.fasterxml.storemate.client.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.api.EntryKeyConverter;
import com.fasterxml.storemate.client.call.CallConfig;
import com.fasterxml.storemate.client.operation.OperationConfig;

public abstract class StoreClientConfig<
    K extends EntryKey,
    CONFIG extends StoreClientConfig<K, CONFIG>
>
{
    // // // Core configuration settings

    protected final EntryKeyConverter<K> _keyConverter;

    protected final ObjectMapper _jsonMapper;
    
    /**
     * Setting that determines whether retries are allowed: usually only
     * disabled for tests.
     */
    protected final boolean _allowRetries;

    protected final OperationConfig _operationConfig;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Life-cycle
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected StoreClientConfig(EntryKeyConverter<K> keyConverter,
            ObjectMapper jsonMapper, boolean allowRetries, OperationConfig operConfig)
    {
        _keyConverter = keyConverter;
        _jsonMapper = jsonMapper;
        _allowRetries = allowRetries;
        _operationConfig = operConfig;
    }

    /**
     * Method to use to create a builder for creating alternate configurations,
     * using base settings from this instance.
     */
    public abstract <BUILDER extends StoreClientConfigBuilder<K, CONFIG, BUILDER>>
        BUILDER builder();
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Accessors
    ///////////////////////////////////////////////////////////////////////
     */

    public EntryKeyConverter<K> getKeyConverter() {
        return _keyConverter;
    }

    public ObjectMapper getJsonMapper() {
    	return _jsonMapper;
    }
    
    public boolean getAllowRetries() { return _allowRetries; }

    /**
     * Accessor for per-call configuration settings.
     */
    public CallConfig getCallConfig() { return _operationConfig.getCallConfig(); }    
    
    public OperationConfig getOperationConfig() { return _operationConfig; }
}
