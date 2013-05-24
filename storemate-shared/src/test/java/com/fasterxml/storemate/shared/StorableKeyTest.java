package com.fasterxml.storemate.shared;

public class StorableKeyTest extends SharedTestBase
{
    public void testPrefix()
    {
        final byte[] b = new byte[] { 1, 2, 3, 4, 5 };
        StorableKey s1 = new StorableKey(b);

        assertTrue(s1.hasPrefix(s1));
        assertTrue(s1.hasPrefix(new StorableKey(b)));
        assertTrue(s1.hasPrefix(new StorableKey(b, 0, 3)));
        assertFalse(s1.hasPrefix(new StorableKey(b, 2, 3)));
        assertEquals(5, s1.length());

        StorableKey s2 = new StorableKey(b, 1, 4);
        assertTrue(s2.hasPrefix(s2));
        assertTrue(s2.hasPrefix(new StorableKey(b, 1, 1)));
        assertTrue(s2.hasPrefix(new StorableKey(b, 1, 2)));
        assertTrue(s2.hasPrefix(new StorableKey(b, 1, 3)));
    }

    public void testRange()
    {
        StorableKey s1 = new StorableKey(new byte[] { 1, 2, 3, 4, 5 });
        StorableKey s2 = s1.range(1, 3);
        assertEquals(3, s2.length());
    }

    public void testSortingWithoutOffsets()
    {
        StorableKey s1 = new StorableKey(new byte[] { 1, 2, 3, 4, 5 });
        StorableKey s2 = new StorableKey(new byte[] { 1, 2, 5, 4, 5 });

        // we happen to report the difference...
        assertEquals(-2, s1.compareTo(s2));
        assertEquals(2, s2.compareTo(s1));

        s1 = new StorableKey(new byte[] { 1, 2, 3, 4 });
        s2 = new StorableKey(new byte[] { 1, 2, 3, 4, 5 });

        assertEquals(-1, s1.compareTo(s2));
        assertEquals(1, s2.compareTo(s1));

        s1 = new StorableKey(new byte[] { 1, 2, 3, 4, 5 });
        s2 = new StorableKey(new byte[] { 1, 2, 3, 4, 5 });
        assertEquals(0, s1.compareTo(s2));
        assertEquals(0, s2.compareTo(s1));
    }

    public void testSortingWithOffsets()
    {
        StorableKey s1 = new StorableKey(new byte[] { 0, 0, 1, 2, 3, 4, 5 }, 2, 5);
        StorableKey s2 = new StorableKey(new byte[] { 1, 2, 5, 4, 5 });

        assertEquals(-2, s1.compareTo(s2));
        assertEquals(2, s2.compareTo(s1));

        s1 = new StorableKey(new byte[] { 1, 2, 3, 4 });
        s2 = new StorableKey(new byte[] { 0, 1, 2, 3, 4, 5 }, 1, 5);

        assertEquals(-1, s1.compareTo(s2));
        assertEquals(1, s2.compareTo(s1));

        // equals within context (ignore trailing)
        s1 = new StorableKey(new byte[] { 0, 1, 2, 3, 4, 5 }, 1, 4);
        s2 = new StorableKey(new byte[] { 0, 1, 2, 3, 4, 6 }, 1, 4);
        assertEquals(0, s1.compareTo(s2));
        assertEquals(0, s2.compareTo(s1));
    }

    public void testSortingWithSignedValues()
    {
        StorableKey s1 = new StorableKey(new byte[] { 1 });
        StorableKey s2 = new StorableKey(new byte[] { -1 }); // 0xFF taken in bigger than 0

        assertEquals(-254, s1.compareTo(s2));
        assertEquals(254, s2.compareTo(s1));
    }

}
