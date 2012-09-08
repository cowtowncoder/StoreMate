package com.fasterxml.storemate.backend.bdbje;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;

import org.joda.time.DateTime;

//import ch.qos.logback.classic.Level;

import com.fasterxml.storemate.backend.bdbje.BDBJEBuilder;

import com.fasterxml.storemate.shared.*;

import com.fasterxml.storemate.store.*;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileManagerConfig;

import com.fasterxml.storemate.shared.SharedTestBase;

/**
 * Base class for unit tests of server sub-module
 */
public abstract class BDBJETestBase extends SharedTestBase
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

    protected StorableStore createStore(String nameSuffix) throws IOException {
    	return createStore(nameSuffix, new TimeMasterForSimpleTesting(123));
    }

    protected StorableStore createStore(String nameSuffix, long startTime) throws IOException {
    	return createStore(nameSuffix, new TimeMasterForSimpleTesting(startTime));
    }
    
    protected StorableStore createStore(String nameSuffix, TimeMaster timeMaster) throws IOException
    {
        File testRoot = getTestScratchDir("bdb-empty-1", true);
        FileManagerConfig fmConfig = new FileManagerConfig(new File(testRoot, "files"));
        StoreConfig cfg = new StoreConfig();
        cfg.dataRootPath = new File(testRoot, "bdb").getCanonicalPath();
        BDBJEBuilder b = new BDBJEBuilder(cfg);
        PhysicalBDBStore physicalStore = b.buildCreateAndInit();
        return new StorableStore(cfg, physicalStore,
                timeMaster, new FileManager(fmConfig, timeMaster));
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

    /*
    ///////////////////////////////////////////////////////////////////////
    // Methods, verifying data
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected void _verifyMetadata(Storable entry, byte[] inputMetadata)
    {
        assertEquals(inputMetadata.length, entry.getMetadataLength());
        byte[] actualMetadata1 = entry.withMetadata(WithBytesAsArray.instance);
        byte[] actualMetadata2 = entry.getMetadata().asBytes();
        assertArrayEquals(inputMetadata, actualMetadata1);
        assertArrayEquals(inputMetadata, actualMetadata2);
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Methods, other
    ///////////////////////////////////////////////////////////////////////
     */
    
    protected static long _date(int year, int month, int day)
    {
        return new DateTime(0L)
                .withYear(year)
                .withMonthOfYear(month)
                .withDayOfMonth(day)
                .withHourOfDay(0)
                .withMinuteOfHour(0)
                .withSecondOfMinute(0)
                .withMillisOfDay(0)
                .getMillis();
    }
    
}