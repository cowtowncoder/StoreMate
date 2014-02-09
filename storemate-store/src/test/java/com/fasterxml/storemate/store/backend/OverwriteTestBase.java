package com.fasterxml.storemate.store.backend;

import java.io.*;

import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.store.BackendTestBase;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableCreationMetadata;
import com.fasterxml.storemate.store.StorableCreationResult;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreOperationSource;
import com.fasterxml.storemate.store.TimeMasterForSimpleTesting;
import com.fasterxml.storemate.store.util.OverwriteChecker;

public abstract class OverwriteTestBase extends BackendTestBase
{
    final long START_TIME_0 = _date(2013, 1, 1);

    // Test case where existing entry is not overwritten
    public void testSimpleOverwriteAlwaysFail() throws Exception {
        _testOverwrite("overwrite-always-fail", false, OverwriteChecker.NeverOkToOverwrite.instance);
    }

    // Test case where existing entry is overwritten due to blanket ok
    public void testSimpleOverwriteAlwaysSuccess() throws Exception {
        _testOverwrite("overwrite-always-ok", true, OverwriteChecker.AlwaysOkToOverwrite.instance);
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

            if (expSuccess) { // yes, ought to overwrite:
                assertTrue("Should succeeed with checker "+checker, resp.succeeded());
                assertNotNull(resp.getPreviousEntry());
            } else { // nope, original retained
                assertFalse("Should fail with checker "+checker, resp.succeeded());
                assertNotNull(resp.getPreviousEntry());
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
            assertTrue(f.exists());
            byte[] b = readFile(f);
            byte[] uncomp = Compressors.lzfUncompress(b);

            
            if (expSuccess) {
                assertEquals(DATA_REPLACE.length, uncomp.length);
            } else {
                assertEquals(DATA_ORIG.length, uncomp.length);
            }
        } finally {
            store.stop();
        }
    }
}