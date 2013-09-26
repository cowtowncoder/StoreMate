package com.fasterxml.storemate.backend.bdbje;

import java.io.*;
import java.util.Arrays;

import com.sleepycat.je.*;
import com.fasterxml.storemate.store.StoreTestBase;

/**
 * Low-level tests that do not use actual Store abstraction.
 */
public class LowLevelTest extends StoreTestBase
{
    protected TestSecKeyCreator _keyCreator;
    
    public void testSecondaryIndexConflict() throws Exception
    {
        // important: Create instance in enabled mode
        _keyCreator = new TestSecKeyCreator();

        final File dataFir = getTestScratchDir("test", true);

        Environment env = new Environment(dataFir, _envConfig());
        Database entries = env.openDatabase(null, "testEntries", _dbConfig(env, true));
        SecondaryDatabase index = env.openSecondaryDatabase(null, "lastModIndex", entries,
                _indexConfig(env, false));

        // Ok. First, add three entries with same time
        entries.put(null, _key("a"), _entry(1234, 1));
        entries.put(null, _key("c"), _entry(1234, 2));
        entries.put(null, _key("b"), _entry(1234, 3));

        // basic verification
        assertEquals(3, entries.count());
        assertEquals(3, index.count());
        DatabaseEntry data = new DatabaseEntry();
        entries.get(null, _key("b"), data, LockMode.DEFAULT);
        assertEquals(8, data.getSize());
        // also, traverse index
        CursorConfig cursorConfig = new CursorConfig();
        SecondaryCursor crsr = index.openCursor(null, cursorConfig);
        data = new DatabaseEntry();
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry primaryKeyEntry = new DatabaseEntry();

        assertEquals(OperationStatus.SUCCESS, crsr.getFirst(keyEntry, primaryKeyEntry, data, null));
        // secondary key only has timestamp, primary key hidden
        assertEquals(4, keyEntry.getSize());
        assertEquals(1, primaryKeyEntry.getSize());
        assertEquals(8, data.getSize()); // primary data
        
        assertEquals(OperationStatus.SUCCESS, crsr.getNext(keyEntry, primaryKeyEntry, data, null));
        assertEquals(OperationStatus.SUCCESS, crsr.getNext(keyEntry, primaryKeyEntry, data, null));
        assertEquals(OperationStatus.NOTFOUND, crsr.getNext(keyEntry, primaryKeyEntry, data, null));

        crsr.close();

        // Then do normal modification
        entries.put(null, _key("c"), _entry(3456, 2));
        assertEquals(3, entries.count());
        assertEquals(3, index.count());
        assertEquals(3, _count(index));

        // and then... let's corrupt things: seems to require TWO operations;
        // first, a failing one
        _keyCreator.startCorruption();
        try {
            entries.put(null, _key("a"), _entry(4567, 2));
            fail("Should have gotten exception");
        } catch (IllegalStateException e) {
            verifyException(e, "Corruption rules");
        }
        // and then one that would succeed
        _keyCreator.stopCorruption();
        try {
            entries.delete(null, _key("a"));
            fail("Ought to fail");
        } catch (SecondaryIntegrityException e) {
            verifyException(e, "Secondary is corrupt");
        }

//        assertEquals(2, entries.count());
//        assertEquals(2, index.count());
//        assertEquals(2, _count(index));

        System.err.println("Corrupt state:");
        _dumpEntries(index);
        index.close();
        entries.close();
        env.close();
       
        // and try re-open
        env = new Environment(dataFir, _envConfig());
        entries = env.openDatabase(null, "testEntries", _dbConfig(env, true));

        // To fix the problem, need to remove, re-add:
        
        env.removeDatabase(null, "lastModIndex");
        index = env.openSecondaryDatabase(null, "lastModIndex", entries,
                _indexConfig(env, true)); // true -> auto-populate

        assertEquals(3, entries.count());
        assertEquals(3, index.count());
        assertEquals(3, _count(index));

        // Now: trying to delete "a" would fail again; so need to try other approaches...
        entries.delete(null, _key("a"));

//        assertTrue(_deleteEntry(index, "a"));

        System.err.println("Fixed state:");
        _dumpEntries(index);
        
        /*
        data = new DatabaseEntry();
        assertEquals(OperationStatus.SUCCESS, entries.get(null, _key("a"), data, null));
        */

        assertEquals(2, entries.count());
        assertEquals(2, index.count());
        assertEquals(2, _count(index));

        index.close();
        entries.close();
        env.close();
    }

    protected void _dumpEntries(SecondaryDatabase db) throws IOException
    {
System.err.println("Entries: ");        
        CursorConfig config = new CursorConfig();
        SecondaryCursor crsr = db.openCursor(null, config);
        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry primaryKeyEntry = new DatabaseEntry();

        OperationStatus status = crsr.getFirst(keyEntry, primaryKeyEntry, data, null);
        while (status == OperationStatus.SUCCESS) {
            System.err.println(" Entry '"+_asString(primaryKeyEntry)+"'; "+_asInt(keyEntry)+" (real: "+_asInt(data, 0)+")");
            status = crsr.getNext(keyEntry, primaryKeyEntry, data, null);
        }
        crsr.close();
System.err.println("<-- Entries");
    }

    protected boolean _deleteEntry(SecondaryDatabase db, String key) throws IOException
    {
        CursorConfig config = new CursorConfig();
        SecondaryCursor crsr = db.openCursor(null, config);
        DatabaseEntry data = new DatabaseEntry();
        
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry primaryKeyEntry = new DatabaseEntry();
        boolean result = false;

        OperationStatus status = crsr.getFirst(keyEntry, primaryKeyEntry, data, null);
        while (status == OperationStatus.SUCCESS) {
            String curr = _rawString(primaryKeyEntry);
            if (curr.equals(key)) {
                System.err.println(" about to delete '"+_asString(primaryKeyEntry)+"'; "+_asInt(keyEntry)+" (real: "+_asInt(data, 0)+")");
                crsr.delete();
                result = true;
                break;
            }
            status = crsr.getNext(keyEntry, primaryKeyEntry, data, null);
        }
        crsr.close();
        return result;
    }
    
    protected String _asString(DatabaseEntry e) throws IOException
    {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        sb.append(e.getSize());
        sb.append(')');
        sb.append(_rawString(e));
        return sb.toString();
    }

    protected String _rawString(DatabaseEntry e) throws IOException
    {
        return new String(e.getData(), e.getOffset(), e.getSize(), "UTF-8");
    }
    
    protected int _count(SecondaryDatabase db)
    {
        CursorConfig config = new CursorConfig();
        SecondaryCursor crsr = db.openCursor(null, config);
        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry primaryKeyEntry = new DatabaseEntry();

        OperationStatus status = crsr.getFirst(keyEntry, primaryKeyEntry, data, null);
        int count = 0;
        while (status == OperationStatus.SUCCESS) {
            ++count;
            status = crsr.getNext(keyEntry, primaryKeyEntry, data, null);
        }
        crsr.close();
        return count;
    }
    
    protected DatabaseEntry _key(String str) throws IOException {
        return new DatabaseEntry(str.getBytes("UTF-8"));
    }

    protected DatabaseEntry _entry(int time, int data) throws IOException {
        byte[] entry = new byte[8];
        _putIntBE(entry, 0, time);
        _putIntBE(entry, 4, data);
        return new DatabaseEntry(entry);
    }

    protected DatabaseEntry _key(int value) {
        byte[] entry = new byte[8];
        _putIntBE(entry, 0, value);
        return new DatabaseEntry(entry);
    }

    protected int _asInt(DatabaseEntry entry, int offset) {
        return _getIntBE(entry.getData(), entry.getOffset() + offset);
    }
    
    protected int _asInt(DatabaseEntry entry) {
        if (entry.getSize() != 4) throw new IllegalArgumentException();
        return _getIntBE(entry.getData(), entry.getOffset());
    }
    
    protected void _putIntBE(byte[] buffer, int offset, int value)
    {
        buffer[offset] = (byte) (value >> 24);
        buffer[++offset] = (byte) (value >> 16);
        buffer[++offset] = (byte) (value >> 8);
        buffer[++offset] = (byte) value;
    }

    protected int _getIntBE(byte[] buffer, int offset)
    {
        return (buffer[offset] << 24)
            | ((buffer[++offset] & 0xFF) << 16)
            | ((buffer[++offset] & 0xFF) << 8)
            | (buffer[++offset] & 0xFF)
            ;
    }

    protected EnvironmentConfig _envConfig()
    {
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(true);
        config.setReadOnly(false);
        return config;
    }

    protected DatabaseConfig _dbConfig(Environment env, boolean allowCreate)
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        EnvironmentConfig econfig = env.getConfig();
        dbConfig.setReadOnly(econfig.getReadOnly());
        dbConfig.setAllowCreate(allowCreate);
        dbConfig.setSortedDuplicates(false);
        dbConfig.setDeferredWrite(false);
        return dbConfig;
    }
    
    protected SecondaryConfig _indexConfig(Environment env, boolean autoPopulate)
    {
        SecondaryConfig secConfig = new SecondaryConfig();
        secConfig.setAllowCreate(env.getConfig().getAllowCreate());
        // should not need to auto-populate?
        secConfig.setAllowPopulate(autoPopulate);
        secConfig.setKeyCreator(_keyCreator);
        // important: timestamps are not unique, need to allow dups:
        secConfig.setSortedDuplicates(true);
        // no, it is not immutable (entries will be updated with new timestamps)
        secConfig.setImmutableSecondaryKey(false);
        secConfig.setDeferredWrite(false);
        return secConfig;
    }

    // Use 4-byte timestamps
    public class TestSecKeyCreator implements SecondaryKeyCreator
    {
        protected boolean _corrupt = false;

        public void startCorruption() { _corrupt = true; }
        public void stopCorruption() { _corrupt = false; }

        @Override
        public boolean createSecondaryKey(SecondaryDatabase secondary,
                DatabaseEntry key, DatabaseEntry data, DatabaseEntry result)
        {
            if (_corrupt) {
                throw new IllegalStateException("Corruption rules supreme!");
            }
            
            // sanity check first:
            byte[] raw = data.getData();
            final int offset = data.getOffset();
            result.setData(Arrays.copyOfRange(raw, offset, offset + 4));
            return true;
        }
    }
}
