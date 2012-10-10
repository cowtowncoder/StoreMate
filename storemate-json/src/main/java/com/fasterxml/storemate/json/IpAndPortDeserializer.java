package com.fasterxml.storemate.json;

import java.io.IOException;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.storemate.shared.IpAndPort;

@SuppressWarnings("serial")
public class IpAndPortDeserializer extends StdScalarDeserializer<IpAndPort>
{
    public IpAndPortDeserializer() { super(IpAndPort.class); }

    @Override
    public IpAndPort deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException, JsonProcessingException
    {
        switch (jp.getCurrentToken()) {
        // should we support anything other than Strings?
        case VALUE_STRING:
            return new IpAndPort(jp.getText());
        default:
        	throw ctxt.mappingException(getValueClass(), jp.getCurrentToken());
        }
    }
}
