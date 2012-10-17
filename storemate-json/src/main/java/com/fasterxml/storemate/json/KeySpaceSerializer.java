package com.fasterxml.storemate.json;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;

import com.fasterxml.storemate.api.KeySpace;

public class KeySpaceSerializer extends StdScalarSerializer<KeySpace>
{
    public KeySpaceSerializer() { super(KeySpace.class); }
    
    @Override
    public void serialize(KeySpace value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonGenerationException
    {
        jgen.writeNumber(value.getLength());
    }

}
