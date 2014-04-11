package com.fasterxml.storemate.shared;

import java.util.Comparator;

/**
 * @since 1.1
 */
public class StorableKeyComparator implements Comparator<StorableKey>
{
    public final static StorableKeyComparator instance = new StorableKeyComparator();
    
    @Override
    public int compare(StorableKey o1, StorableKey o2) {
        return o1.compareTo(o2);
    }
}
