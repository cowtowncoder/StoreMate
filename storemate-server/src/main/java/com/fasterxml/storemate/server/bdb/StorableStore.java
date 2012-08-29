package com.fasterxml.storemate.server.bdb;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.server.file.FileManager;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.WithBytesCallback;

import com.sleepycat.je.*;

/**
 * Simple abstraction for storing "decorated BLOBs", with a single
 * secondary index that can be used for traversing entries by
 * "last modified" time.
 */
public class StorableStore
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final KeyConverter KEY_CONV = new KeyConverter();
    
    /*
    /**********************************************************************
    /* Simple config
    /**********************************************************************
     */

    protected final File _dataRoot;
    
    /*
    /**********************************************************************
    /* External helper objects
    /**********************************************************************
     */

//    protected final TimeMaster _timeMaster;

    protected final FileManager _fileManager;

    /*
    /**********************************************************************
    /* BDB entities
    /**********************************************************************
     */

    /**
     * Underlying primary BDB-JE database
     */
    protected final Database _entries;

    /**
     * Secondary database that tracks last-modified order of primary entries.
     */
    protected final SecondaryDatabase _index;
    
    /*
    /**********************************************************************
    /* Store status
    /**********************************************************************
     */

    protected final AtomicBoolean _closed = new AtomicBoolean(false);

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    public StorableStore(File dbRoot, FileManager fileManager,
            Database entryDB, SecondaryDatabase lastModIndex)
    {
        _fileManager = fileManager;
        _dataRoot = dbRoot;
        _entries = entryDB;
        _index = lastModIndex;
    }

    public void stop()
    {
        if (!_closed.getAndSet(true)) {
            Environment env = _entries.getEnvironment();
            _index.close();
            _entries.close();
            env.close();
        }
    }
    
    /*
    /**********************************************************************
    /* API, simple-ish accessors
    /**********************************************************************
     */

    public boolean isClosed() {
        return _closed.get();
    }

    /*
    /**********************************************************************
    /* API, data reads
    /**********************************************************************
     */

    public boolean hasEntry(StorableKey key)
    {
        _checkClosed();
        OperationStatus status = _entries.get(null, dbKey(key), null, LockMode.READ_COMMITTED);
        switch (status) {
        case SUCCESS:
        case KEYEXIST:
            return true;
        case KEYEMPTY: // was deleted during operation.. shouldn't be getting
        case NOTFOUND:
            // fall through
        }
        return false;
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected void _checkClosed()
    {
        if (_closed.get()) {
            throw new IllegalStateException("Can not access data from StorableStore after it has been closed");
        }
    }

    protected DatabaseEntry dbKey(StorableKey key)
    {
        return key.with(KEY_CONV);
    }
    
    private final static class KeyConverter implements WithBytesCallback<DatabaseEntry>
    {
        @Override
        public DatabaseEntry withBytes(byte[] buffer, int offset, int length) {
            if (offset == 0 && length == buffer.length) {
                return new DatabaseEntry(buffer);
            }
            return new DatabaseEntry(buffer, offset, length);
        }
    }
}
