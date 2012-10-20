package com.fasterxml.storemate.shared;

import com.fasterxml.storemate.shared.ByteRange;

public class ByteRangeTest extends SharedTestBase
{
    public void testSimpleValid()
    {
        String src = "bytes=0-500";
        ByteRange range = ByteRange.valueOf(src);
        assertEquals(0, range.getStart());
        assertEquals(500, range.getEnd());
        assertEquals(src, range.toString());
        assertEquals("bytes 0-500", range.asRequestHeader());
        assertEquals("bytes 0-500/*", range.asResponseHeader());

        src = "bytes=-2";
        range = ByteRange.valueOf(src);
        assertEquals(-2, range.getStart());
        // end not really defined...
        assertEquals(-1, range.getEnd());
        assertEquals(src, range.toString());
        range = range.resolveWithTotalLength(100);
        assertEquals("bytes 98-99", range.asRequestHeader());
        assertEquals("bytes 98-99/100", range.asResponseHeader());

        // also, may add total length indicator; if so, should be included
        src = "bytes=0-499/1500";
        range = ByteRange.valueOf(src);
        assertEquals(0, range.getStart());
        assertEquals(499, range.getEnd());
        assertEquals(1500, range.getTotalLength());
        assertEquals("bytes 0-499", range.asRequestHeader());
        assertEquals("bytes 0-499/1500", range.asResponseHeader());
    }

    public void testSimpleInvalid()
    {
        try {
            ByteRange.valueOf("bytes=3");
            fail();
        } catch (IllegalArgumentException e) {
            verifyException(e, "no hyphen foun");
        }
        try {
            ByteRange.valueOf("bytes=100-0");
            fail();
        } catch (IllegalArgumentException e) {
            verifyException(e, "end can not be less");
        }
    }
}
