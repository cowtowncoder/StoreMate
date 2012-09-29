package com.fasterxml.storemate.json;

import java.io.IOException;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;

import com.fasterxml.storemate.shared.key.KeyRange;

public class KeyRangeDeserializer extends StdScalarDeserializer<KeyRange>
{
    public KeyRangeDeserializer() { super(KeyRange.class); }

    @Override
    public KeyRange deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException
    {
        /*
        switch (jp.getCurrentToken()) {
        case VALUE_STRING:
            int i = jp.getValueAsInt(-1);
            if (i > 0) {
                return new KeyRange(jp.getValueAsInt());
            }
            break;
        case VALUE_NUMBER_INT:
            return new KeyRange(jp.getIntValue());
        default:
        }
        */
        throw ctxt.mappingException(getValueClass(), jp.getCurrentToken());
    }
}
