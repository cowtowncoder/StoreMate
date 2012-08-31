package com.fasterxml.storemate.store.file;

import java.io.*;

import org.joda.time.DateTime;

public class DirByTime
{
    protected final File _dir;
    
    protected final DateTime _createTime;

    public DirByTime(File dir, DateTime createTime)
    {
        _dir = dir;
        _createTime = createTime;
    }

    public File getDirectory() { return _dir; }
    
    public long getRawCreateTime() {
        return _createTime.getMillis();
    }

    /**
     * Method called to delete this directory along with all of
     * its contents.
     */
    public int nuke(FileCleanupStats stats) {
        return _nukeDir(_dir, stats);
    }

    /**
     * Method called to find and delete all empty directories
     * within this directory; as well as directory itself
     * if empty after clean up.
     * 
     * @return True if this directory was empty and removed.
     */
    public boolean removeEmpty(FileCleanupStats stats) {
        return _removeEmpty(_dir, stats);
    }
    
    protected int _nukeDir(File dir, FileCleanupStats stats)
    {
        int failed = 0;
        int childCount = 0;
        for (File f : dir.listFiles()) {
            ++childCount;
            if (f.isDirectory()) {
                failed += _nukeDir(f, stats);
            } else {
                if (f.delete()) {
                    stats.addDeletedFile();
                } else {
                    ++failed;
                }        
            }
        }
        if (failed == 0) {
            if (dir.delete()) {
                if (childCount == 0) {
                    stats.addDeletedEmptyDir();
                } else {
                    stats.addDeletedDir();
                }
            } else {
                ++failed;
            }
        }
        return failed;
    }

    protected boolean _removeEmpty(File dir, FileCleanupStats stats)
    {
        int remaining = 0;
        for (File f : dir.listFiles()) {
            if (f.isDirectory()) {
                if (_removeEmpty(f, stats)) {
                    stats.addDeletedEmptyDir();
                    continue;
                }
                stats.addRemainingDir();
            }
            ++remaining;
        }
        if (remaining > 0) {
            return false;
        }
        return dir.delete();
    }
    
    @Override
    public String toString() { return _dir.getAbsolutePath(); }
}
