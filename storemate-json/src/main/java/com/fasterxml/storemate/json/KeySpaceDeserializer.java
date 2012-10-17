package com.fasterxml.storemate.json;

import java.io.IOException;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;

import com.fasterxml.storemate.service.KeySpace;

@SuppressWarnings("serial")
public class KeySpaceDeserializer extends StdScalarDeserializer<KeySpace>
{
    public KeySpaceDeserializer() { super(KeySpace.class); }

    @Override
    public KeySpace deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException
    {
        switch (jp.getCurrentToken()) {
        case VALUE_STRING:
            int i = jp.getValueAsInt(-1);
            if (i > 0) {
                return new KeySpace(jp.getValueAsInt());
            }
            break;
        case VALUE_NUMBER_INT:
            return new KeySpace(jp.getIntValue());
        default:
        }
        throw ctxt.mappingException(getValueClass(), jp.getCurrentToken());
    }
}
