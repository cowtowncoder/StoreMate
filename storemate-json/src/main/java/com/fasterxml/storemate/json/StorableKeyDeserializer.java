package com.fasterxml.storemate.json;

import java.io.IOException;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.storemate.shared.StorableKey;

public class StorableKeyDeserializer extends StdScalarDeserializer<StorableKey>
{
    public StorableKeyDeserializer() {
        super(StorableKey.class);
    }

    @Override
    public StorableKey deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException
    {
        switch (jp.getCurrentToken()) {
        case VALUE_STRING:
        case VALUE_EMBEDDED_OBJECT:
            byte[] raw = jp.getBinaryValue();
            return new StorableKey(raw);
        default:
            throw ctxt.mappingException(getValueClass(), jp.getCurrentToken());
        }
    }
}
