package com.fasterxml.storemate.store;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;

import org.joda.time.DateTime;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;
import com.fasterxml.storemate.shared.util.UTF8Encoder;
import com.fasterxml.storemate.shared.util.WithBytesAsArray;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.file.DefaultFilenameConverter;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileManagerConfig;
import com.fasterxml.storemate.store.impl.StorableStoreImpl;

public abstract class BackendTestBase extends StoreTestBase
{
    /**
     * Method to be called before tests, to ensure log4j does not whine.
     */
    protected void initTestLogging()
    {
    }

    @Override
    public void setUp() {
        initTestLogging();
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Test methods: factory methods
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public StorableKey storableKey(String str) {
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
        File testRoot = getTestScratchDir(nameSuffix, true);
        File fileDir = new File(testRoot, "files");
        StoreConfig storeConfig = new StoreConfig();
        FileManagerConfig fmConfig = new FileManagerConfig(fileDir);
        StoreBackend backend = createBackend(testRoot, storeConfig);
        return new StorableStoreImpl(storeConfig, backend, timeMaster,
                new FileManager(fmConfig, timeMaster, new DefaultFilenameConverter()),
                null, null);
    }

    protected abstract StoreBackend createBackend(File testRoot, StoreConfig storeConfig);

    /*
    ///////////////////////////////////////////////////////////////////////
    // Methods, verifying data
    ///////////////////////////////////////////////////////////////////////
     */

    protected void _verifyCounts(long exp, StorableStore store) throws StoreException
    {
        _verifyEntryCount(exp, store);
        _verifyIndexCount(exp, store);
    }
    
    protected void _verifyEntryCount(long exp, StorableStore store)
        throws StoreException
    {
        StoreBackend backend = store.getBackend();
        if (backend.hasEfficientEntryCount()) {
            assertEquals(exp, backend.getEntryCount());
        }
        // but let's also verify via actual iteration?
        assertEquals(exp, backend.countEntries());
    }

    protected void _verifyIndexCount(long exp, StorableStore store)
        throws StoreException
    {
        StoreBackend backend = store.getBackend();
        if (backend.hasEfficientIndexCount()) {
            assertEquals(exp, backend.getIndexedCount());
        }
        // but let's also verify via actual iteration?
        assertEquals(exp, backend.countIndexed());
    }
    
    protected void _verifyMetadata(Storable entry, byte[] inputMetadata)
    {
        assertEquals(inputMetadata.length, entry.getMetadataLength());
        byte[] actualMetadata1 = entry.withMetadata(WithBytesAsArray.instance);
        byte[] actualMetadata2 = entry.getMetadata().asBytes();
        assertArrayEquals(inputMetadata, actualMetadata1);
        assertArrayEquals(inputMetadata, actualMetadata2);
    }

    protected void _verifyHash(byte[] data, int exp, String msg)
    {
        int act = BlockMurmur3Hasher.instance.hash(data);
        if (act != exp) {
              assertEquals(msg, Integer.toHexString(exp), Integer.toHexString(act));
        }
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
