package com.fasterxml.storemate.shared;

import java.io.ByteArrayInputStream;

public class ThrottlingByteArrayInputStream extends ByteArrayInputStream
{
	protected final int _maxBytesPerCall;
	
	public ThrottlingByteArrayInputStream(byte[] data, int maxBytes) {
		this(data, 0, data.length, maxBytes);
	}

	public ThrottlingByteArrayInputStream(byte[] data, int offset, int length,
			int maxBytes) {
		super(data, offset, length);
		_maxBytesPerCall = maxBytes;
	}

	@Override
	public int read(byte[] buffer) {
		return read(buffer, 0, buffer.length);
	}

	@Override
	public int read(byte[] buffer, int offset, int length) {
		if (length > _maxBytesPerCall) {
			length = _maxBytesPerCall;
		}
		return super.read(buffer, 0, buffer.length);
	}
}

