package com.fasterxml.storemate.store.lastaccess;

public abstract class LastAccessConverter<K, E, ACC extends LastAccessUpdateMethod>
{
    public abstract long findLastAccessedTime(E entry);

//    public abstract long findLastAccessedTime(K key, ACC method);

//    public abstract EntryLastAccessed findLastAccessed(K key, ACC method);

    public abstract EntryLastAccessed createLastAccessed(E entry, long timestamp);

    public abstract EntryLastAccessed createLastAccessed(byte[] raw, int offset, int length);
    
    public abstract byte[] createLastAccessedKey(K key, ACC method);

    public abstract byte[] createLastAccessedKey(E entry);
}
