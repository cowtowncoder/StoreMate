package com.fasterxml.storemate.json;

import java.io.IOException;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;

import com.fasterxml.storemate.api.ClientId;

@SuppressWarnings("serial")
public class ClientIdDeserializer extends StdScalarDeserializer<ClientId>
{
    public ClientIdDeserializer() { super(ClientId.class); }

    @Override
    public ClientId deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException
    {
        switch (jp.getCurrentToken()) {
        case VALUE_STRING:
            return ClientId.valueOf(jp.getText());
        case VALUE_NUMBER_INT:
            return ClientId.valueOf(jp.getIntValue());
        default:
        	throw ctxt.mappingException(getValueClass(), jp.getCurrentToken());
        }
    }
}
