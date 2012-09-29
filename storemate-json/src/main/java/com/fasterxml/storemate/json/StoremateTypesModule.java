package com.fasterxml.storemate.json;

import com.fasterxml.jackson.core.Version;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleSerializers;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.key.KeyRange;
import com.fasterxml.storemate.shared.key.KeySpace;

public class StoremateTypesModule extends Module
{
    public StoremateTypesModule() { }

    @Override
    public String getModuleName() {
        return "StoremateTypes";
    }

    @Override
    public Version version() {
        return Version.unknownVersion();
    }

    @Override
    public void setupModule(SetupContext context)
    {
        SimpleDeserializers desers = new SimpleDeserializers();
        SimpleSerializers sers = new SimpleSerializers();

        desers.addDeserializer(IpAndPort.class, new IpAndPortDeserializer());
        sers.addSerializer(IpAndPort.class, new IpAndPortSerializer());
//        desers.addDeserializer(KeyRange.class, new KeyRangeDeserializer());
        sers.addSerializer(KeyRange.class, new KeyRangeSerializer());
        desers.addDeserializer(KeySpace.class, new KeySpaceDeserializer());
        sers.addSerializer(KeySpace.class, new KeySpaceSerializer());
        desers.addDeserializer(StorableKey.class, new StorableKeyDeserializer());
        sers.addSerializer(StorableKey.class, new StorableKeySerializer());

        context.addDeserializers(desers);
        context.addSerializers(sers);
    }
}
