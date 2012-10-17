package com.fasterxml.storemate.json;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Sub-class of {@link ObjectMapper} that has custom handlers for
 * datatypes used by standard StoreMate servers and clients.
 */
@SuppressWarnings("serial")
public class StoreMateObjectMapper extends ObjectMapper
{
    public StoreMateObjectMapper()
    {
        // since these are JSON mappers, no point in numeric representation (false)
        registerModule(new StoremateTypesModule(false));
    }
}
