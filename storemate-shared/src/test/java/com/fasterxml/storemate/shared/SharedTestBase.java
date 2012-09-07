package com.fasterxml.storemate.shared;

import java.util.Random;

import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;

import junit.framework.TestCase;

/**
 * Base class for unit tests
 */
public abstract class SharedTestBase extends TestCase
{
    /*
    ///////////////////////////////////////////////////////////////////////
    // Checksum calculation
    ///////////////////////////////////////////////////////////////////////
     */

	protected int calcChecksum32(byte[] data) {
		return calcChecksum32(data, 0, data.length);
	}

	protected int calcChecksum32(byte[] data, int offset, int len ) {
		return BlockMurmur3Hasher.hash(0, data, offset, len);
	}
	
	/*
    ///////////////////////////////////////////////////////////////////////
    // Data generation methods
    ///////////////////////////////////////////////////////////////////////
     */
	
    protected String biggerCompressibleData(int size)
    {
        StringBuilder sb = new StringBuilder(size + 100);
        final Random rnd = new Random(123);
        while (sb.length() < size) {
            sb.append("Some data: ")
            .append(sb.length())
            .append("/")
            .append(sb.length())
            .append(rnd.nextInt()).append("\n");
        }
        return sb.toString();
    }

    protected String biggerSomewhatCompressibleData(int size)
    {
        StringBuilder sb = new StringBuilder(size + 100);
        final Random rnd = new Random(123);
        while (sb.length() < size) {
            int val = rnd.nextInt();
            switch (val % 5) {
            case 0:
                sb.append('X');
                break;
            case 1:
                sb.append(": ").append(sb.length());
                break;
            case 2:
                sb.append('\n');
                break;
            case 3:
                sb.append((char) (33 + val & 0x3f));
                break;
            default:
                sb.append("/").append(Integer.toHexString(sb.length()));
                break;
            }
        }
        return sb.toString();
    }
    
    protected String biggerRandomData(int size)
    {
        StringBuilder sb = new StringBuilder(size + 100);
        Random rnd = new Random(size);
        for (int i = 0; i < size; ++i) {
            sb.append((byte) (32 + rnd.nextInt() % 95));
        }
        return sb.toString();
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Exception verification methods
    ///////////////////////////////////////////////////////////////////////
     */

    protected final void verifyException(Exception e, String expected)
    {
        verifyMessage(expected, e.getMessage());
    }
    
    protected final void verifyMessage(String expectedPiece, String actual)
    {
        if (actual == null || actual.toLowerCase().indexOf(expectedPiece.toLowerCase()) < 0) {
            fail("Expected message that contains phrase '"+expectedPiece+"'; instead got: '"
                    +actual+"'");
        }
    }
}