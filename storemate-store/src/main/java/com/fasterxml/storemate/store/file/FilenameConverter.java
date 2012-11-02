package com.fasterxml.storemate.store.file;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Simple helper class we use for cleaning up external path names
 * into clean(er) internal file names.
 * Since there is no requirement to be able to reverse the transformation,
 * clean-up process can often be performed starting with raw byte-based
 * keys.
 */
public abstract class FilenameConverter
{
    /**
     * Method to call to convert given
     * {@link rawKey} into filename that is safe with respect to
     * quotable characters.
     */
    public abstract String createFilename(StorableKey rawKey);

    /**
     * Method to call to append filename created from given
     * {@link rawKey} in given {@link StringBuilder}.
     */
    public abstract StringBuilder appendFilename(StorableKey rawKey, final StringBuilder sb);
}
