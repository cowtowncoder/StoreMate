package com.fasterxml.storemate.store;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

//import ch.qos.logback.classic.Level;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.shared.UTF8Encoder;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileManagerConfig;

/**
 * Base class for unit tests of server sub-module
 */
public abstract class StoreTestBase extends TestCase
{
    /**
     * Method to be called before tests, to ensure log4j does not whine.
     */
    protected void initTestLogging()
    {
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Test methods: factory methods
    ///////////////////////////////////////////////////////////////////////
     */

    public StorableKey storableKey(String str)
    {
        return new StorableKey(UTF8Encoder.encodeAsUTF8(str));
    }

    protected StorableStore createStore(String nameSuffix) throws IOException
    {
        final TimeMaster timeMaster = new TimeMasterForSimpleTesting(123);
        File testRoot = getTestScratchDir("bdb-empty-1", true);
        FileManagerConfig fmConfig = new FileManagerConfig(new File(testRoot, "files"));
        StoreConfig cfg = new StoreConfig();
        cfg.dataRootPath = new File(testRoot, "bdb").getCanonicalPath();
        StoreBuilder b = new StoreBuilder(cfg, timeMaster, new FileManager(fmConfig, timeMaster));
        return b.buildCreateAndInit();
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Test methods: exception verification
    ///////////////////////////////////////////////////////////////////////
     */

    protected void verifyException(Exception e, String expected)
    {
        verifyMessage(expected, e.getMessage());
    }
    
    protected void verifyMessage(String expectedPiece, String actual)
    {
        if (actual == null || actual.toLowerCase().indexOf(expectedPiece.toLowerCase()) < 0) {
            fail("Expected message that contains phrase '"+expectedPiece+"'; instead got: '"
                    +actual+"'");
        }
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Methods for file, directory handling
    ///////////////////////////////////////////////////////////////////////
     */
        
    /**
     * Method for accessing "scratch" directory used for tests.
     * We'll try to create this directory under 
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