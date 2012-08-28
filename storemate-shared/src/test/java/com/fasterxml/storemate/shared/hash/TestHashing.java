package com.fasterxml.storemate.shared.hash;

import java.util.Random;

import com.fasterxml.storemate.shared.SharedTestBase;

public class TestHashing extends SharedTestBase
{
	private final static int SEED = 0;
	
	private final byte[] TEST_DATA1;
	private final byte[] TEST_DATA2;
	private final byte[] TEST_DATA3;
	private final byte[] TEST_DATA4;

	public TestHashing() throws Exception
	{
		final String base = "abcdefghjiklmnopqrstuvwxyz01234567890";

		final int baseLen = base.length();

		TEST_DATA1 = base.getBytes("UTF-8");
		TEST_DATA2 = base.substring(0, baseLen-1).getBytes("UTF-8");
		TEST_DATA3 = base.substring(0, baseLen-2).getBytes("UTF-8");
		TEST_DATA4 = base.substring(0, baseLen-3).getBytes("UTF-8");
	}
	
	public void testIncrementalMurmur3OnePass()
	{
		_verifyMurmur3(TEST_DATA1);
		_verifyMurmur3(TEST_DATA2);
		_verifyMurmur3(TEST_DATA3);
		_verifyMurmur3(TEST_DATA4);
	}

	public void testIncrementalMurmur3ByteByByte()
	{
		_verifyMurmur3OneByOne(TEST_DATA1);
		_verifyMurmur3OneByOne(TEST_DATA2);
		_verifyMurmur3OneByOne(TEST_DATA3);
		_verifyMurmur3OneByOne(TEST_DATA4);
	}

	public void testIncrementalMurmur3WithVariable()
	{
		_verifyMurmur3WithVariable(TEST_DATA1);
		_verifyMurmur3WithVariable(TEST_DATA2);
		_verifyMurmur3WithVariable(TEST_DATA3);
		_verifyMurmur3WithVariable(TEST_DATA4);
	}

	/*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */
	
	private void _verifyMurmur3(byte[] data)
	{
		int exp = StdMurmur3Hasher.hash(SEED, data, 0, data.length);
		
		IncrementalMurmur3Hasher hasher = new IncrementalMurmur3Hasher(SEED);
		hasher.update(data, 0, data.length);
		assertEquals(data.length, (int) hasher.getLength());
		
		int act = hasher.calculateHash();
		// sanity check: should be stable:
		assertEquals(act, hasher.calculateHash());
		assertEquals(exp, act);
	}

	private void _verifyMurmur3OneByOne(byte[] data)
	{
		int exp = StdMurmur3Hasher.hash(SEED, data, 0, data.length);

		byte[] buffer = new byte[5];

		IncrementalMurmur3Hasher hasher = new IncrementalMurmur3Hasher(SEED);
		for (int i = 0; i < data.length; ++i) {
			int off = (i % buffer.length);
			buffer[off] = data[i];
			hasher.update(buffer, off, 1);
			// just for fun, calculate hash along the way too, to verify it won't break things
			hasher.calculateHash();
		}
		assertEquals(data.length, (int) hasher.getLength());
		
		int act = hasher.calculateHash();
		assertEquals(exp, act);
	}

	private void _verifyMurmur3WithVariable(byte[] data)
	{
		int exp = StdMurmur3Hasher.hash(SEED, data, 0, data.length);

		IncrementalMurmur3Hasher hasher = new IncrementalMurmur3Hasher(SEED);
		Random rnd = new Random(data.length);
		
		for (int i = 0; i < data.length; ) {
			int amount = 1 + rnd.nextInt(17);
			amount = Math.min(amount, data.length - i);
			byte[] buffer = new byte[amount];
			System.arraycopy(data, i, buffer, 0, amount);
			hasher.update(buffer, 0, amount);
			i += amount;
			// just for fun, calculate hash along the way too, to verify it won't break things
			hasher.calculateHash();
		}
		// and then verify we still get expected hash code...
		assertEquals(data.length, (int) hasher.getLength());
		
		int act = hasher.calculateHash();
		assertEquals(exp, act);
	}
}
