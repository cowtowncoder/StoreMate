package com.fasterxml.storemate.shared.util;

import java.io.*;

import com.fasterxml.storemate.shared.SharedTestBase;
import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;
import com.fasterxml.storemate.shared.hash.IncrementalMurmur3Hasher;

public class TestByteAggregator extends SharedTestBase
{
    public void testSmallAndSimple()
    {
        ByteAggregator aggr = new ByteAggregator();
        aggr.write(1);
        aggr.write(2);
        aggr.write(3);
        assertEquals(3, aggr.size());
        byte[] b = aggr.toByteArray(false);
        assertEquals(3, b.length);
        assertEquals(1, b[0]);
        assertEquals(2, b[1]);
        assertEquals(3, b[2]);
        assertEquals(3, aggr.size());

        b = aggr.toByteArray(); // defaults to passing 'true', i.e. reset contents
        assertEquals(3, b.length);
        assertEquals(0, aggr.size());

        aggr.close();
    }

    public void testBigger() throws Exception
    {
        final int LEN = 500 * 1024;
        ByteAggregator aggr = new ByteAggregator();
        final byte[] buf = new byte[123];
        int offset = 0;
        for (int i = 0; i < LEN; ++i) {
            buf[offset++] = (byte) i;
            if (offset == buf.length) {
                aggr.write(buf);
                offset = 0;
            }
        }
        if (offset > 0) {
            aggr.write(buf, 0, offset);
        }
        assertEquals(LEN, aggr.size());
        
        byte[] b = aggr.toByteArray(false);
        assertEquals(LEN, b.length);
        for (int i = 0; i < LEN; ++i) {
            assertEquals(i & 0xFF, b[i] & 0xFF);
        }

        int hash = BlockMurmur3Hasher.instance.hash(b);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        aggr.writeTo(bytes);
        assertEquals(LEN, bytes.size());

        // also: better calculate hash/checksum correctly
        final IncrementalMurmur3Hasher hasher = new IncrementalMurmur3Hasher();
        aggr.calcChecksum(hasher);
        assertEquals(hash, hasher.calculateHash());
        
        aggr.close();
    }

    public void testReadUpTo() throws Exception
    {
        final int len = 3 * 1000 * 1000;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(len);
        for (int i = 0; i < len; ++i) {
            bytes.write((byte) i);
        }

        ByteAggregator aggr = new ByteAggregator();
        final int len2 = len/3;

        ByteArrayInputStream in = new ByteArrayInputStream(bytes.toByteArray());
        for (int i = 0; i < 16; ++i) {
            aggr.write((byte) in.read());
        }

        int count = aggr.readUpTo(in, len2-16);
        assertEquals(len2-16, count);

        byte[] output = aggr.toByteArray();
        assertEquals(len2, output.length);
        aggr.close();
        
        for (int i = 0; i < len2; ++i) {
            assertEquals((byte) i, output[i]);
        }
    }
}
