package com.fasterxml.storemate.store.file;

import com.fasterxml.storemate.store.StoreTestBase;
import com.fasterxml.storemate.store.file.FilenameConverter;

public class TestFilenames extends StoreTestBase
{
	public void testNameMangling() throws Exception
	{
		FilenameConverter conv = new FilenameConverter('@');
		assertEquals("this@here@and@there", conv.createFilename(storableKey("this/here and there")));
	}
}
