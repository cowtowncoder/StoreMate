package com.fasterxml.storemate.store.file;

import com.fasterxml.storemate.store.ServerTestBase;
import com.fasterxml.storemate.store.file.FilenameConverter;

public class TestFilenames extends ServerTestBase
{
	public void testNameMangling() throws Exception
	{
		FilenameConverter conv = new FilenameConverter('@');
		assertEquals("this@here@and@there", conv.createFilename(storableKey("this/here and there")));
	}
}
