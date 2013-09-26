package com.fasterxml.storemate.store;

import java.io.File;
import java.io.IOException;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.SharedTestBase;
import com.fasterxml.storemate.shared.util.UTF8Encoder;

/**
 * Base class for unit tests of server sub-module
 */
public abstract class StoreTestBase extends SharedTestBase
{
    public StorableKey storableKey(String str)
    {
        return new StorableKey(UTF8Encoder.encodeAsUTF8(str));
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Methods for file, directory handling
    ///////////////////////////////////////////////////////////////////////
     */
        
    /**
     * Method for accessing "scratch" directory used for tests.
     * We'll try to create this directory under current directory.
     * Assumption is that the current directory at this point
     * is project directory.
     */
    protected File getTestScratchDir(String testSuffix, boolean cleanUp) throws IOException
    {
        File f = new File(new File(".test-storage"), testSuffix).getCanonicalFile();
        if (!f.exists()) {
            if (!f.mkdirs()) {
                throw new IOException("Failed to create test directory '"+f.getAbsolutePath()+"'");
            }
        } else if (cleanUp) {
            for (File kid : f.listFiles()) {
                deleteFileOrDir(kid);
            }
        }
        return f;
    }

    protected void deleteFileOrDir(File fileOrDir) throws IOException
    {
        if (fileOrDir.isDirectory()) {
            for (File kid : fileOrDir.listFiles()) {
                deleteFileOrDir(kid);
            }
        }
        if (!fileOrDir.delete()) {
            throw new IOException("Failed to delete test file/directory '"+fileOrDir.getAbsolutePath()+"'");
        }
    }
}