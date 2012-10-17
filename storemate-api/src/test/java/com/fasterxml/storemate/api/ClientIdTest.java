package com.fasterxml.storemate.api;

import com.fasterxml.storemate.api.ClientId;
import com.fasterxml.storemate.shared.SharedTestBase;

public class ClientIdTest extends SharedTestBase
{
    public void testNumeric()
    {
        // First, simple valid ones
        assertEquals(0, ClientId.valueOf(0).asInt());
        assertEquals(123, ClientId.valueOf(123).asInt());
        assertEquals(99999, ClientId.valueOf(99999).asInt());

        // then invalid
        try {
            ClientId.valueOf(-1);
            fail();
        } catch (IllegalArgumentException e) {
            verifyException(e, "can not be negative number");
        }

        try {
            ClientId.valueOf(ClientId.MAX_NUMERIC + 1);
            fail();
        } catch (IllegalArgumentException e) {
            verifyException(e, "can not exceed "+ClientId.MAX_NUMERIC);
        }
    }

    public void testNumericAsText()
    {
        ClientId id;

        // Also: can pass numbers in range...
        assertEquals(0, ClientId.valueOf("0").asInt());
        id = ClientId.valueOf("1");
        assertEquals(1, id.asInt());
        assertEquals("1", id.toString());
        id = ClientId.valueOf("999999");
        assertEquals(999999, id.asInt());
        assertEquals("999999", id.toString());

        // leading zeroes are fine as well; will be stripped
        id = ClientId.valueOf("00012");
        assertEquals(12, id.asInt());
        assertEquals("12", id.toString());

        // and then invalid cases
        try {
            ClientId.valueOf(String.valueOf(ClientId.MAX_NUMERIC + 1));
            fail();
        } catch (IllegalArgumentException e) {
            verifyException(e, "can not exceed "+ClientId.MAX_NUMERIC);
        }

        try {
            ClientId.valueOf(String.valueOf("12345678901234567890"));
            fail();
        } catch (IllegalArgumentException e) {
            verifyException(e, "can not exceed "+ClientId.MAX_NUMERIC);
        }
    }
    
    public void testSymbolic()
    {
        // First, simple valid ones
        assertEquals(0, ClientId.valueOf("").asInt());
        assertEquals(0, ClientId.valueOf(null).asInt());
        ClientId id = ClientId.valueOf("A123");
        assertEquals("A123", id.toString());
        assertEquals(0x41313233, id.asInt());
        assertEquals(id, ClientId.valueOf("A123"));
        // as well as starting with number
        assertEquals(id, ClientId.valueOf(0x41313233));

        id = ClientId.valueOf("ZZZZ");
        assertEquals("ZZZZ", id.toString());
        assertEquals(0x5a5a5a5a, id.asInt());

        id = ClientId.valueOf("ABC_");
        assertEquals("ABC_", id.toString());
        assertEquals(0x4142435F, id.asInt());
        
        // then invalid
        try {
            ClientId.valueOf("ABC");
            fail();
        } catch (IllegalArgumentException e) {
            verifyException(e, "has to be 4 characters");
        }
        try {
            ClientId.valueOf("Abcd");
            fail();
        } catch (IllegalArgumentException e) {
            verifyException(e, "letter, number");
        }
        try {
            ClientId.valueOf("_ABC");
            fail();
        } catch (IllegalArgumentException e) {
            verifyException(e, "does not start with");
        }
    }
}
