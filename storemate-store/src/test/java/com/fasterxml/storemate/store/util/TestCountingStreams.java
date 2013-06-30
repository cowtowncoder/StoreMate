package com.fasterxml.storemate.store.util;

import java.io.*;

import com.fasterxml.storemate.shared.hash.IncrementalHasher32;
import com.fasterxml.storemate.shared.hash.IncrementalMurmur3Hasher;
import com.fasterxml.storemate.store.StoreTestBase;

public class TestCountingStreams extends StoreTestBase
{
    public void testCountingInputStream() throws Exception
    {
        byte[] INPUT = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8  };
        CountingInputStream in = new CountingInputStream(new ByteArrayInputStream(INPUT), new IncrementalMurmur3Hasher());

        assertEquals(1, in.read());
        byte[] buffer = new byte[8];
        assertEquals(4, in.read(buffer, 2, 4));
        for (int i = 0; i < 4; ++i) {
            assertEquals(2+i, buffer[2+i]);
        }
        assertEquals(6, in.read());
        assertEquals(2L, in.skip(100));
        assertEquals(-1, in.read());
        assertEquals(6L, in.readCount());
        assertEquals(2L, in.skipCount());

        // and then hash... have to count 
        int exp = calcChecksum32(INPUT, 0, 6);
        IncrementalHasher32 hasher = in.getHasher();
        assertNotNull(hasher);
        assertEquals(exp, hasher.calculateHash());
        
        in.close();
    }

    public void testCountingOutputStream() throws Exception
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CountingOutputStream out = new CountingOutputStream(bytes, new IncrementalMurmur3Hasher());
        
        byte[] INPUT = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8  };
        out.write(INPUT, 0, 7);
        out.write(8);

        assertEquals(8, out.count());
        int exp = calcChecksum32(INPUT);
        IncrementalHasher32 hasher = out.getHasher();
        assertNotNull(hasher);
        assertEquals(exp, hasher.calculateHash());
        
        out.close();
    }
}
