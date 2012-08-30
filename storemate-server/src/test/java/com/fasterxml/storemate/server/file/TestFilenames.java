package com.fasterxml.storemate.server.file;

import com.fasterxml.storemate.server.ServerTestBase;
import com.fasterxml.storemate.server.file.FilenameConverter;

public class TestFilenames extends ServerTestBase
{
	public void testNameMangling() throws Exception
	{
		FilenameConverter conv = new FilenameConverter('@');
		assertEquals("this@here@and@there", conv.createFilename(storableKey("this/here and there")));
	}
}
