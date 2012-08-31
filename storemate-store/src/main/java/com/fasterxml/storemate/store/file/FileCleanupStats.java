package com.fasterxml.storemate.store.file;

/**
 * Helper class used to keep track of clean up progress
 * for file system cleanup.
 */
public class FileCleanupStats
{
    // Number of directories skipped due to non-matching name
    protected int skippedDirs = 0;
    // Number of directories deleted (along with contents, if any)
    protected int deletedDirs = 0;
    // Number of empty directories seen (and deleted)
    protected int deletedEmptyDirs = 0;
    // Number of matching non-expired directories left after traversal
    protected int remainingDirs = 0;

    // Number of files deleted (in non-empty directories)
    protected int deletedFiles = 0;

    public void addSkippedDir() { ++skippedDirs; }
    public void addRemainingDir() { ++remainingDirs; }
    
    public void addDeletedEmptyDir() { ++deletedEmptyDirs; }
    public void addDeletedDir() { ++deletedDirs; }

    public void addDeletedFile() { ++deletedFiles; }

    @Override
    public String toString()
    {
        return new StringBuilder(60)
            .append("Skipped ").append(skippedDirs)
            .append(", deleted ").append(deletedDirs)
            .append(" and left ").append(remainingDirs)
            .append(" directories; deleted ").append(deletedFiles).append(" files")
            .toString();
    }
}
