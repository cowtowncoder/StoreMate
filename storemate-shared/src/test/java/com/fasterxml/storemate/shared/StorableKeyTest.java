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

        StorableKey s2 = new StorableKey(b, 1, 4);
        assertTrue(s2.hasPrefix(s2));
        assertTrue(s2.hasPrefix(new StorableKey(b, 1, 1)));
        assertTrue(s2.hasPrefix(new StorableKey(b, 1, 2)));
        assertTrue(s2.hasPrefix(new StorableKey(b, 1, 3)));
    }
}
