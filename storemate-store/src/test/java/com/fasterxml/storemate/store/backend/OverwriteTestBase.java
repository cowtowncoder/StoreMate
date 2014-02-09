package com.fasterxml.storemate.store.backend;

import java.io.*;
import java.util.*;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.store.BackendTestBase;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableCreationMetadata;
import com.fasterxml.storemate.store.StorableCreationResult;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.StoreOperationSource;
import com.fasterxml.storemate.store.TimeMasterForSimpleTesting;
import com.fasterxml.storemate.store.util.OverwriteChecker;

public abstract class OverwriteTestBase extends BackendTestBase
{
    final long START_TIME_0 = _date(2013, 1, 1);

    // Test case where existing entry is not overwritten
    public void testSimpleOverwriteFails() throws Exception {
        _testOverwrite("overwrite-always-fail", false, OverwriteChecker.NeverOkToOverwrite.instance);
    }

    // Test case where existing entry is overwritten due to blanket ok
    public void testSimpleOverwriteSucceeds() throws Exception {
        _testOverwrite("overwrite-always-ok", true, OverwriteChecker.AlwaysOkToOverwrite.instance);
    }

    public void testConditionalOverwriteFails() throws Exception {
        _testOverwrite("overwrite-conditional-fail", false, new OverwriteIfOlder());
    }

    public void testConditionalOverwriteSucceeds() throws Exception {
        _testOverwrite("overwrite-conditional-ok", true, new OverwriteIfNewer());
    }
    
    private void _testOverwrite(String name, boolean expSuccess, OverwriteChecker checker)
        throws Exception
    {
        TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(START_TIME_0);
        StorableStore store = createStore("bdb-"+name, timeMaster);
        final StorableKey KEY1 = storableKey("data/overwrite1");
        // big enough not to get inlined
        final byte[] DATA_ORIG = biggerCompressibleData(200 * 1000).getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_ORIG = new byte[] { 'a', 'b', 'c', 'd' };

        final byte[] DATA_REPLACE = biggerCompressibleData(190 * 1000).getBytes("UTF-8");
        final byte[] CUSTOM_METADATA_REPLACE = new byte[] { 'd', 'e' };
        
        try {
            StorableCreationMetadata metadata = new StorableCreationMetadata(null,
                            calcChecksum32(DATA_ORIG), HashConstants.NO_CHECKSUM);
            metadata.uncompressedSize = -1L;
            StorableCreationResult resp = store.insert(StoreOperationSource.REQUEST, null,
                    KEY1, new ByteArrayInputStream(DATA_ORIG),
                    metadata, ByteContainer.simple(CUSTOM_METADATA_ORIG));
            assertTrue(resp.succeeded());
            
            assertNull(resp.getPreviousEntry());
            _verifyCounts(1L, store);

            // Then try to overwrite with a newer timestamp
            timeMaster.advanceCurrentTimeMillis(1000);

            StorableCreationMetadata metadata2 = new StorableCreationMetadata(null,
                    calcChecksum32(DATA_REPLACE), HashConstants.NO_CHECKSUM);
            metadata.uncompressedSize = -1L;
            resp = store.upsertConditionally(StoreOperationSource.REQUEST, null,
                    KEY1, new ByteArrayInputStream(DATA_REPLACE),
                    metadata2, ByteContainer.simple(CUSTOM_METADATA_REPLACE),
                    true, checker);

            _verifyCounts(1L, store);

            final Storable oldEntry = resp.getPreviousEntry();
            assertNotNull(oldEntry);
            assertTrue(oldEntry.hasExternalData());
            
            if (expSuccess) { // yes, ought to overwrite:
                assertTrue("Should succeeed with checker "+checker, resp.succeeded());
            } else { // nope, original retained
                assertFalse("Should fail with checker "+checker, resp.succeeded());
            }

            // and then verify
            Storable entry = store.findEntry(StoreOperationSource.REQUEST, null, KEY1);
            assertNotNull(entry);
            assertFalse(entry.hasInlineData());
            assertTrue(entry.hasExternalData());
            assertEquals(Compression.LZF, entry.getCompression());

            if (expSuccess) {
                assertEquals(START_TIME_0 + 1000L, entry.getLastModified());
                assertEquals(DATA_REPLACE.length, entry.getOriginalLength());
                assertEquals(DATA_REPLACE.length, entry.getActualUncompressedLength());
                _verifyMetadata(entry, CUSTOM_METADATA_REPLACE);
            } else {
                assertEquals(START_TIME_0, entry.getLastModified());
                assertEquals(DATA_ORIG.length, entry.getOriginalLength());
                assertEquals(DATA_ORIG.length, entry.getActualUncompressedLength());
                _verifyMetadata(entry, CUSTOM_METADATA_ORIG);
            }
            // and read the contents
            File f = entry.getExternalFile(store.getFileManager());
            if (!f.exists()) {
                fail("File '"+f.getAbsolutePath()+"' (replacement? "+expSuccess+") should exist, does not");
            }
            byte[] b = readFile(f);
            byte[] uncomp = Compressors.lzfUncompress(b);
            
            if (expSuccess) {
                assertEquals(DATA_REPLACE.length, uncomp.length);
            } else {
                assertEquals(DATA_ORIG.length, uncomp.length);
            }

            // And finally, verify that old file is gone, if replacement succeeded
            if (expSuccess) {
                File oldFile = oldEntry.getExternalFile(store.getFileManager());
                if (oldFile.exists()) {
                    fail("Should have deleted old file '"+oldFile.getAbsolutePath()+"' ("
                            +oldFile.length()+" bytes)");
                }
            }
            // Regardless, should have one and only one storage file remaining.
            File dir = store.getFileManager().dataRootForTesting();
            List<String> filenames = new ArrayList<String>();
            _findFiles(dir, filenames);
            if (filenames.size() != 1) {
                fail("Should only have 1 store file after operations, got "+filenames.size()+": "+filenames);
            }
         } finally {
            store.stop();
        }
    }

    private void _findFiles(File dir, List<String> filenames) throws IOException
    {
        if (!dir.isDirectory()) {
            filenames.add(dir.getAbsolutePath());
        } else {
            for (File f : dir.listFiles()) {
                _findFiles(f, filenames);
            }
        }
    }
    
    private static class OverwriteIfNewer implements OverwriteChecker {
        @Override public Boolean mayOverwrite(StorableKey key) { return null; }
        
        @Override public boolean mayOverwrite(StorableKey key, Storable oldEntry, Storable newEntry)
            throws StoreException {
            return newEntry.getLastModified() > oldEntry.getLastModified();
        }
    }

    private static class OverwriteIfOlder implements OverwriteChecker {
        @Override public Boolean mayOverwrite(StorableKey key) { return null; }
        
        @Override public boolean mayOverwrite(StorableKey key, Storable oldEntry, Storable newEntry)
            throws StoreException {
            return newEntry.getLastModified() < oldEntry.getLastModified();
        }
    }
}
