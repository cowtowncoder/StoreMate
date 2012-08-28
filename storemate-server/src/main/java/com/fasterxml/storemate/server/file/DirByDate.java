package com.fasterxml.storemate.server.file;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Object that represents the main-level data directory, directly
 * under data file root directory.
 * Sortable in ascending direction, that is, order by time
 * of creation.
 */
public class DirByDate implements Comparable<DirByDate>
{
    private final static Logger LOG = LoggerFactory.getLogger(DirByDate.class);

    /**
     * Pattern used for the main-level date-based directories
     */
    final static Pattern DATE_DIR_PATTERN = Pattern.compile("(\\d\\d\\d\\d)\\-(\\d\\d)\\-(\\d\\d)");

    final static Pattern TIME_DIR_PATTERN = Pattern.compile("(\\d\\d):(\\d\\d)");
    
    protected final File _dir;
    
    protected final DateTime _createTime;

    public DirByDate(File dir, DateTime createTime)
    {
        _dir = dir;
        _createTime = createTime;
    }

    public static List<DirByDate> listMainDataDirs(File root, FileCleanupStats stats)
    {
        ArrayList<DirByDate> dirs = new ArrayList<DirByDate>();
        for (File dir : root.listFiles()) {
            Matcher m = DATE_DIR_PATTERN.matcher(dir.getName());
            if (m.matches()) {
                try {
                    int year = Integer.parseInt(m.group(1));
                    int month = Integer.parseInt(m.group(2));
                    int day = Integer.parseInt(m.group(3));
                    MutableDateTime created = new MutableDateTime(0L, DateTimeZone.UTC);
                    created.setTime(0, 0, 0, 0);
                    created.setDate(year, month, day);
                    dirs.add(new DirByDate(dir, created.toDateTime()));
                    continue;
                } catch (IllegalArgumentException e) {
                    LOG.warn("Invalid directory name {}, will skip", dir.getAbsolutePath());
                }
            }
            if (stats != null) {
                stats.addSkippedDir();
            }
        }
        // and then sort...
        Collections.sort(dirs);
        return dirs;
    }
    
    public File getDirectory() { return _dir; }
    
    public List<DirByTime> listTimeDirs(FileCleanupStats stats) //throws IOException
    {
        ArrayList<DirByTime> dirs = new ArrayList<DirByTime>();
        for (File dir : _dir.listFiles()) {
            Matcher m = TIME_DIR_PATTERN.matcher(dir.getName());
            if (m.matches()) {
                try {
                    int hour = Integer.parseInt(m.group(1));
                    int minute = Integer.parseInt(m.group(2));
                    dirs.add(new DirByTime(dir,
                        _createTime
                            .withHourOfDay(hour)
                            .withMinuteOfHour(minute)));
                    continue;
                } catch (IllegalArgumentException e) {
                    LOG.warn("Invalid directory name {}, will skip", dir.getAbsolutePath());
                }
            }
            if (stats != null) {
                stats.addSkippedDir();
            }
        }
        return dirs;
    }
    
    public long getRawCreateTime() {
        return _createTime.getMillis();
    }
    
    @Override
    public int compareTo(DirByDate other)
    {
        long thisTime = getRawCreateTime();
        long thatTime = other.getRawCreateTime();

        if (thisTime < thatTime) {
            return -1;
        }
        if (thisTime > thatTime) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() { return _dir.getAbsolutePath(); }
}
