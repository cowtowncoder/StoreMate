package com.fasterxml.storemate.json;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.WithBytesCallback;

public class StorableKeySerializer extends StdScalarSerializer<StorableKey>
{
    public StorableKeySerializer() {
        super(StorableKey.class);
    }
    
    @Override
    public void serialize(StorableKey value, final JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonGenerationException
    {
        IOException e = value.with(new WithBytesCallback<IOException>() {
            @Override
            public IOException withBytes(byte[] buffer, int offset, int length) {
                try {
                    jgen.writeBinary(buffer, offset, length);
                } catch (IOException e) {
                    return e;
                }
                return null;
            }
            
        });
        if (e != null) {
            throw e;
        }
    }
}
