package com.fasterxml.storemate.store.file;

import java.io.*;
import java.util.*;

import org.joda.time.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.shared.compress.Compression;

/**
 * Handler class that takes care of mapping external logical path
 * into physical path to use for storing large content BLOBs (that is,
 * content that is NOT inlined in database).
 *<p>
 * Some things worth noting on mapping:
 *<ul>
 * <li>We will be creating files sequentially on directories, to keep size
 *   of each directory manageable (typically something like 1000 or so)
 *  </li>
 * <li>All directory and file names are in ASCII
 *  </li>
 * <li>We can try optimizing file names for BDB prefix elimination: as a result,
 *    things that differ (sequence id) should be added as suffixes
 *  </li>
 * <li>For convenience, a little bit of metadata is retained in filenames, such
 *   as compression type (last suffix)
 *  </li>
 *</ul>
 */
public class FileManager
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    /**
     * We need to use a sequence number (usually 4 digits, possibly more),
     * and simple compression prefix; this is in addition to basename of
     * the file (from external key). But also there's the preceding
     * file path, which adds almost 20 chars.
     */
    private final static int FILENAME_OVERHEAD = 32;
    
    /**
     * Rounding for number of minutes, when constructing date-based
     * branch directories. Currently we round by 5 minutes, to prevent
     * number of such directories from growing beyond 1000 per day.
     */
    private final static int MINUTE_MODULO = 5;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Helper objects
    ///////////////////////////////////////////////////////////////////////
     */

    protected final TimeMaster _timeMaster;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */

    protected final FilenameConverter _filenameConverter;
    
    /**
     * Root directory under which data directories are dynamically
     * created.
     */
    protected final File _dataRoot;

    /**
     * And we also need to sometimes prepend data root as String
     */
    protected final String _dataRootPath;

    /**
     * How many files will we store in individual directories?
     */
    protected final int _maxFilesPerDir;

    protected final int _maxFilenameBaseLength;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // State
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Directory (under {@link #_dataRoot}) that is currently used as the
     * active branch, and is named based on time when it was created.
     * Relative to root, something like "2012-02-25/13:15"
     */
    protected File _dateBranch;
	
    /**
     * Timestamp of time when we should advance to a new {@link #_dateBranch}
     */
    protected long _nextDateCheck;

    /**
     * Child directory under {@link #_dateBranch} where files are actively
     * being written.
     */
    protected File _activeBranch;

    /**
     * Numeric part of the active branch, used for roll over.
     */
    protected int _activeBranchId;

    /**
     * String to use as the prefix when storing path relative to storage root.
     */
    protected String _relativeActivePath;

    /**
     * Per-directory running file number; used for ensuring that we don't
     * fill directories too full
     */
    protected int _fileIndex;
	
    /*
    /**********************************************************************
    // Construction
    /**********************************************************************
     */
	
    /**
     * @param filenameConverter Converter to use (for example,
     *   {@link DefaultFilenameConverter}
     */
    public FileManager(FileManagerConfig config, TimeMaster timeMaster,
            FilenameConverter filenameConverter)
    {
        _timeMaster = timeMaster;
        _filenameConverter = filenameConverter;
        final long now = timeMaster.currentTimeMillis();
        File dataRoot = config.dataRoot;
        if (dataRoot == null) {
            throw new IllegalArgumentException("Missing 'dataRoot' configuration value for FileManager");
        }
        if (!dataRoot.exists()) { // create?
            if (!dataRoot.mkdirs()) {
                throw new IllegalStateException("Data directory '"+dataRoot.getAbsolutePath()+"' did not exist, failed to create");
            }
        }
        _dataRoot = dataRoot;
        String abs = dataRoot.getAbsolutePath();
        if (!abs.endsWith("/")) {
            abs += "/";
        }
        _dataRootPath = abs;
        _calculateDateBranch(now);
        _maxFilesPerDir = config.maxFilesPerDirectory;

        /* We will need to reserve bit of space for sequence number,
         * suffixes; 12 chars should be enough
         */
        if (config.maxFilenameLength <= FILENAME_OVERHEAD) {
        	throw new IllegalStateException("Too low setting for 'maxFilenameLength' ("
        			+config.maxFilenameLength+"): must be at least "+(FILENAME_OVERHEAD+1));
        }
        _maxFilenameBaseLength = config.maxFilenameLength - FILENAME_OVERHEAD;
    }

    /*
    /**********************************************************************
    /* Public API for storing
    /**********************************************************************
     */

    /**
     * Method used for finding file to store new data entry in, such that
     * we maintain balanced directory structure; retain most important
     * metadata in name, and avoid collisions.
     */
    public FileReference createStorageFile(StorableKey extKey, Compression compressed,
            long creationTime)
    {
        int index;
        synchronized (this) {
            // First: do we need to roll over to a new Date-based dir?
            if (creationTime > _nextDateCheck) {
                _calculateDateBranch(creationTime);
            }
            // if not, is there room in the current dir?
            if (_fileIndex >= _maxFilesPerDir) {
                _fileIndex = 0;
                ++_activeBranchId;
                _activeBranch = new File(_dateBranch, String.format("%03d", _activeBranchId));
                // should we verify no just dir exists? For now yes, but only WARN
                if (_activeBranch.exists()) { // fine, let's use it
                    LOG.warn("Directory {} already exists: will still use as a Data dir...",
                            _activeBranch.getAbsolutePath());
                } else {
                    _activeBranch.mkdirs();
                }
                _calculateActivePath();
            }
            index = _fileIndex;
            ++_fileIndex;
        }
        String cleanName = buildFilename(extKey, index, compressed);

        // Ok, so let's construct our reference...
        return new FileReference(new File(_activeBranch, cleanName),
                _relativeActivePath + cleanName);
    }

    public File derefenceFile(String relativePath)
    {
        if (relativePath == null) { // inlined data?
            return null;
        }
        return new File(_dataRootPath + relativePath);
    }

    /*
    /**********************************************************************
    /* Public API for clean up
    /**********************************************************************
     */
    
    /**
     * Method that will find and return all main-level data directories,
     * in ascending order from the oldest to newest.
     */
    public List<DirByDate> listMainDataDirs(FileCleanupStats stats)
    {
        return DirByDate.listMainDataDirs(_dataRoot, stats);
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
	
    /**
     * Helper method called to handle embedded problematic characters.
     * For now, assume that only slashes are problematic; and since we use
     * URL escaping, percents as well.
     */
    protected final String buildFilename(StorableKey key, int index,
            Compression comp)
    {
        if (comp == null) {
            comp = Compression.NONE;
        }
    	// We will try to create name like "ORIG-NAME-MANGLED.[seqNr][comprType]"
    	
    	int keyLen = key.length();
        StringBuilder sb = new StringBuilder(FILENAME_OVERHEAD
        		+ Math.min(_maxFilenameBaseLength, keyLen));
        sb = _filenameConverter.appendFilename(key, sb);
    	if (sb.length() > _maxFilenameBaseLength) {
    		// could get fancy, trying to preserve suffix etc, but simple does it for now
    		sb.setLength(_maxFilenameBaseLength);
    	}
        // and then something like ".289G" 
        sb.append('.')
            .append(index)
            .append(comp.asChar())
            ;
        return sb.toString();
    }
    
    protected void _calculateDateBranch(long timestamp)
    {
        // Joda defaults to ISO chronology, local timezone; force use of UTC
        DateTime dt = new DateTime(timestamp, DateTimeZone.UTC);
        // round down to preceding minute marker
        int min = dt.getMinuteOfHour();
        int mod = min % MINUTE_MODULO;
        if (mod != 0) {
            dt = dt.withMinuteOfHour(min - mod);
        }
        // calculate path like "YYYY-MM-DD/HH:MM" (with minute rounded down)
        File f = new File(_dataRoot, String.format("%04d-%02d-%02d",
                dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth()));
        _dateBranch = new File(f, String.format("%02d:%02d", dt.getHourOfDay(), dt.getMinuteOfHour()));
        // and calculate next rollover time as well
        _nextDateCheck = dt.plusMinutes(MINUTE_MODULO).getMillis();

        // Next thing: find active tip
        int id = 0;

        while (true) {
            f = new File(_dateBranch, String.format("%03d", id));
            if (!f.exists()) { // fine, let's use it
                f.mkdirs();
                break;
            }
            // otherwise no, skip existing dirs
            ++id;
            if (id > 49999) { // sanity check
                throw new IllegalStateException("Failed to find name for active tip directory under '"
                        +_dateBranch.getAbsolutePath()+": tried until "+id);
            }
        }
        _activeBranch = f;
        _activeBranchId = id;
        _fileIndex = 0;
        _calculateActivePath();
    }

    protected void _calculateActivePath()
    {
        StringBuilder sb = new StringBuilder();
        // Ok: we need 3 levels of dirs to have something like
        // "2012-02-19/15:35/001"
        File p = _activeBranch.getParentFile();
        _relativeActivePath = sb.append(p.getParentFile().getName())
                .append('/')
                .append(p.getName())
                .append('/')
                .append(_activeBranch.getName())
                .append('/')
                .toString();
    }
}
