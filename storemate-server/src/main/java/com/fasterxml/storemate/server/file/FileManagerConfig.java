package com.fasterxml.storemate.server.file;

/**
 * Simple value class used for binding configuration settings used
 * with {@link FileManager}
 */
public class FileManagerConfig
{
    /*
    /**********************************************************************
    /* Simple config properties
    /**********************************************************************
     */
	
	/**
	 * Maximum number of data files to store in a single directory,
	 * before having to construct a new one.
	 *<p>
	 * Default value is chosen below value used by Unix shell to truncate
	 * directory listing output (which is usually 1024)
	 */
	public int maxFilesPerDirectory = 1000;

	/**
	 * Maximum length of filenames to create for data entries
	 *<p>
	 * Default value chosen assuming typical Unix file system limit of 255
	 * bytes.
	 */
	public int maxFilenameLength = 240;
	
	/**
	 * Name of root directory (using relative or absolute path) under which
	 * actual data directories will be created.
	 */
	public String dataRootPath;

	/**
	 * {@link FilenameConverter} implementation to use, if any
	 */
	public Class<? extends FilenameConverter> filenameConverter = FilenameConverter.class;
	
	/*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */

	public FilenameConverter createFilenameConverter()
	{
		if (filenameConverter == null || filenameConverter == FilenameConverter.class) {
			return new FilenameConverter();
		}
		try {
			return (FilenameConverter) filenameConverter.newInstance();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to instantiate FilenameConverter of type "
					+filenameConverter+": "+e, e);
		}
	}
}
