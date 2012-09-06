package com.fasterxml.storemate.store.util;

import org.junit.Assert;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.store.StoreTestBase;

public class TestBytesAndStuff extends StoreTestBase
{
    public void testSimple()
    {
        // first, trivial estimation
        StuffToBytes est = StuffToBytes.estimator();
        est.appendVInt(Integer.MAX_VALUE);
        est.appendVLong(Long.MAX_VALUE);
        final byte[] EMPTY5 = new byte[5];
        
        est.appendLengthAndBytes(ByteContainer.simple(EMPTY5));
        assertEquals(20, est.offset());

        // then write it out
        StuffToBytes w = StuffToBytes.writer(20);
        w.appendVInt(Integer.MAX_VALUE);
        w.appendVLong(Long.MAX_VALUE);
        assertEquals(14, w.offset());
        w.appendLengthAndBytes(ByteContainer.simple(new byte[5]));
        assertEquals(20, w.offset());

        // and most importantly, read back in
        byte[] stuff = w.bufferedBytes().asBytes();
        assertEquals(20, stuff.length);
        BytesToStuff reader = new BytesToStuff(stuff);
        
        assertEquals(Integer.MAX_VALUE, reader.nextVInt());
        assertEquals(Long.MAX_VALUE, reader.nextVLong());
        
        assertEquals(5, reader.nextVInt());
        byte[] b = reader.nextBytes(5);
        Assert.assertArrayEquals(EMPTY5, b);
        assertEquals(stuff.length, reader.offset());
    }
}
