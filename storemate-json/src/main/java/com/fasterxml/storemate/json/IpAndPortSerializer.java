package com.fasterxml.storemate.json;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import com.fasterxml.storemate.shared.IpAndPort;

public class IpAndPortSerializer extends StdScalarSerializer<IpAndPort>
{
    public IpAndPortSerializer() {
        super(IpAndPort.class);
    }
    
    @Override
    public void serialize(IpAndPort value, final JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonGenerationException
    {
        jgen.writeString(value.toString());
    }
}
