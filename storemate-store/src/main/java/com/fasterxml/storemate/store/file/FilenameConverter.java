package com.fasterxml.storemate.store.file;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.WithBytesCallback;

/**
 * Simple helper class we use for cleaning up external path names
 * into clean(er) internal file names. Anything considered non-clean
 * we will simply convert to a replacement character: uniqueness will
 * be guaranteed by other means, and this is just a simple best-effort
 * thing to retain some knowledge of the original name.
 * All non-ASCII characters are considered unclean as well.
 */
public class FilenameConverter
{
	public final static char DEFAULT_SAFE_CHAR = '_';
	
	protected final static int[] DEFAULTS;
	static {
		int[] def = new int[256];
		// slash/backslash, double-quotes are bad... anything else?
		final String REMOVE = "/\"\\";
		
		// only allow things above Space (32), in ASCII range. And skip DEL (127)
		for (int i = 33; i < 0x7F; ++i) { // ctrl chars and Space
			char c = (char) i;
			if (REMOVE.indexOf(c) < 0) {
				def[i] = 1;
			}
		}
		DEFAULTS = def;
	}

	protected final char _safeChar;

	public FilenameConverter() {
		this(DEFAULT_SAFE_CHAR);
	}
	
	public FilenameConverter(char safeChar) {
		_safeChar = safeChar;
	}
	
	protected boolean isSafe(byte b)
	{
		return DEFAULTS[b & 0xFF] != 0;
	}

	public String createFilename(StorableKey rawKey)
	{
		StringBuilder sb = new StringBuilder();
		return appendFilename(rawKey, sb).toString();
	}

	public StringBuilder appendFilename(StorableKey rawKey, final StringBuilder sb)
	{
	    rawKey.with(new WithBytesCallback<Void>() {
	        @Override
	        public Void withBytes(byte[] buffer, int offset, int length) {
	            final int end = offset+length;
	            while (offset < end) {
	                byte b = buffer[offset++];
	                sb.append(isSafe(b) ? (char)b : _safeChar);
	            }
	            return null;
	        }
	    });
	    return sb;
	}
}
