package com.fasterxml.storemate.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.Version;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleSerializers;

import com.fasterxml.storemate.api.ClientId;
import com.fasterxml.storemate.api.KeyRange;
import com.fasterxml.storemate.api.KeySpace;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.StorableKey;

public class StoremateTypesModule extends Module
{
    protected final boolean _cfgUseNumerics;

    public StoremateTypesModule(boolean useNumerics) {
    	_cfgUseNumerics = useNumerics;
    }

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
        // for KeyRange, only manual serializer; deserializer with mix-ins
        sers.addSerializer(KeyRange.class, new KeyRangeSerializer());
        desers.addDeserializer(KeySpace.class, new KeySpaceDeserializer());
        sers.addSerializer(KeySpace.class, new KeySpaceSerializer());
        desers.addDeserializer(StorableKey.class, new StorableKeyDeserializer());
        sers.addSerializer(StorableKey.class, new StorableKeySerializer());
        desers.addDeserializer(ClientId.class, new ClientIdDeserializer());
        sers.addSerializer(ClientId.class, new ClientIdSerializer(_cfgUseNumerics));

        context.addDeserializers(desers);
        context.addSerializers(sers);

        // Plus mix-ins cover structured types:
        context.setMixInAnnotations(KeyRange.class, KeyRangeMixins.class);
    }

    /*
    /**********************************************************************
    /* Helper types for mix-ins
    /**********************************************************************
     */

    /**
     * Instead of trying to write a manual deserializer for KeyRange, let's
     * use mix-ins; this way no annotations are needed there, but things
     * will work as if they were.
     */
    abstract static class KeyRangeMixins extends KeyRange
    {
        @JsonCreator
        protected KeyRangeMixins(External ext) { super(ext); }
    }
}
