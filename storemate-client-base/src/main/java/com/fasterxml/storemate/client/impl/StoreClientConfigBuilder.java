package com.fasterxml.storemate.client.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.storemate.api.EntryKey;
import com.fasterxml.storemate.api.EntryKeyConverter;
import com.fasterxml.storemate.client.call.CallConfig;
import com.fasterxml.storemate.client.operation.OperationConfig;
import com.fasterxml.storemate.json.StoreMateObjectMapper;

/**
 * Builder class for creating immutable {@link StoreClientConfig}
 * instances.
 */
public abstract class StoreClientConfigBuilder<K extends EntryKey,
    CONFIG extends StoreClientConfig<K, CONFIG>,
    BUILDER extends StoreClientConfigBuilder<K, CONFIG, BUILDER>
>
{
    /*
    ///////////////////////////////////////////////////////////////////////
    // Default values
    ///////////////////////////////////////////////////////////////////////
     */

    protected final static OperationConfig DEFAULT_OPERATION_CONFIG = new OperationConfig();

    protected final static ObjectMapper DEFAULT_JSON_MAPPER = new StoreMateObjectMapper();
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // state
    ///////////////////////////////////////////////////////////////////////
     */

    protected EntryKeyConverter<K> _keyConverter;

    // We need some additional features; but a single instance is fine:
    protected ObjectMapper _jsonMapper = DEFAULT_JSON_MAPPER;
    
    // // // General configuration
    
    /**
     * Setting that determines whether retries are allowed: usually only
     * disabled for tests.
     */
    protected boolean _allowRetries;

    // // // For CallConfig

    // // Single call timeouts
    
    protected long _connectTimeoutMsecs;

    protected long _putCallTimeoutMsecs;

    protected long _getCallTimeoutMsecs;

    protected long _deleteCallTimeoutMsecs;

    protected int _maxExcerptLength;
    
    // // // For OperationConfig
    
    // // High-level operation settings, OK calls to require/try
    
    protected int _minOksToSucceed;

    protected int _optimalOks;

    protected int _maxOks;
    
    // // Operation timeouts
    
    protected long _putOperationTimeoutMsecs;

    protected long _getOperationTimeoutMsecs;

    protected long _deleteOperationTimeoutMsecs;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Instance creation, building
    ///////////////////////////////////////////////////////////////////////
     */
    
    public StoreClientConfigBuilder(EntryKeyConverter<K> keyConverter) {
        this(keyConverter, DEFAULT_JSON_MAPPER,
        		DEFAULT_OPERATION_CONFIG, true);
    }

    public StoreClientConfigBuilder(CONFIG config)
    {
        this(config.getKeyConverter(), config.getJsonMapper(),
        		config.getOperationConfig(), config.getAllowRetries());
    }

    protected StoreClientConfigBuilder(EntryKeyConverter<K> keyConv, ObjectMapper jsonMapper,
            OperationConfig operationConfig, boolean allowRetries)
    {
        _keyConverter = keyConv;
        _jsonMapper = jsonMapper;
        _allowRetries = allowRetries;

        _minOksToSucceed = operationConfig.getMinimalOksToSucceed();
        _optimalOks = operationConfig.getOptimalOks();
        _maxOks = operationConfig.getMaxOks();
        
        _putOperationTimeoutMsecs = operationConfig.getPutOperationTimeoutMsecs();
        _getOperationTimeoutMsecs = operationConfig.getGetOperationTimeoutMsecs();
        _deleteOperationTimeoutMsecs = operationConfig.getDeleteOperationTimeoutMsecs();
        
        final CallConfig callConfig = operationConfig.getCallConfig();
        _connectTimeoutMsecs = callConfig.getConnectTimeoutMsecs();
        _putCallTimeoutMsecs = callConfig.getPutCallTimeoutMsecs();
        _getCallTimeoutMsecs = callConfig.getGetCallTimeoutMsecs();
        _deleteCallTimeoutMsecs = callConfig.getDeleteCallTimeoutMsecs();
    }

    /**
     * Main build method; needs to be abstract for sub-classes to produce
     * custom config objects.
     */
    public abstract CONFIG build();

    protected OperationConfig buildOperationConfig() {
        CallConfig cc = buildCallConfig();
        return new OperationConfig(cc,
                _minOksToSucceed, _optimalOks, _maxOks,
                _putOperationTimeoutMsecs, _getOperationTimeoutMsecs, _deleteOperationTimeoutMsecs
        );
    }

    protected CallConfig buildCallConfig() {
        return new CallConfig(_connectTimeoutMsecs,
                _putCallTimeoutMsecs, _getCallTimeoutMsecs, _deleteCallTimeoutMsecs,
                _maxExcerptLength);                
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Mutators
    ///////////////////////////////////////////////////////////////////////
     */

    @SuppressWarnings("unchecked")
    public BUILDER setAllowRetries(boolean enable) {
        _allowRetries = enable;
        return (BUILDER) this;
    }

    // // // Low-level single-call timeouts
    
    @SuppressWarnings("unchecked")
    public BUILDER setConnectTimeoutMsecs(long msecs) {
        _connectTimeoutMsecs = msecs;
        return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setPutCallTimeoutMsecs(long msecs) {
        _putCallTimeoutMsecs = msecs;
        return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setGetCallTimeoutMsecs(long msecs) {
        _getCallTimeoutMsecs = msecs;
        return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setDeleteCallTimeoutMsecs(long msecs) {
        _deleteCallTimeoutMsecs = msecs;
        return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setMaxExcerptLength(int max) {
        _maxExcerptLength = max;
        return (BUILDER) this;
    }
    
    // // // High-level operation settings
    
    @SuppressWarnings("unchecked")
    public BUILDER setMinimalOksToSucceed(int count) {
        _minOksToSucceed = count;
        return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setOptimalOks(int count) {
        _optimalOks = count;
        return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setMaxOks(int count) {
        _maxOks = count;
        return (BUILDER) this;
    }

    // // // Operation timeouts
    
    @SuppressWarnings("unchecked")
    public BUILDER setPutOperationTimeoutMsecs(long msecs) {
        _putOperationTimeoutMsecs = msecs;
        return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setGetOperationTimeoutMsecs(long msecs) {
        _getOperationTimeoutMsecs = msecs;
        return (BUILDER) this;
    }

    @SuppressWarnings("unchecked")
    public BUILDER setDeleteOperationTimeoutMsecs(long msecs) {
        _deleteOperationTimeoutMsecs = msecs;
         return (BUILDER) this;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Accessors
    ///////////////////////////////////////////////////////////////////////
     */

    public EntryKeyConverter<K> getKeyConverter() {
        return _keyConverter;
    }

    public boolean getAllowRetries() { return _allowRetries; }

    public ObjectMapper getJsonMapper() {
    	return _jsonMapper;
    }
}
