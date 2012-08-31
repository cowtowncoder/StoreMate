package com.fasterxml.storemate.store.file;

import java.io.*;

/**
 * Helper class that encapsulates information on a single data file.
 */
public class FileReference
{
    /**
     * Reference to actual storage file
     */
    public final File _file;

    /**
     * Relative file path that gets stored in DB
     */
    public final String _relativeReference;

    public FileReference(File f, String relRef)
    {
        _file = f;
        _relativeReference = relRef;
    }

    public File getFile() { return _file; }
    public String getReference() { return _relativeReference; }
}
