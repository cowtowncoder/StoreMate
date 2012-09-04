package com.fasterxml.storemate.store.util;

public interface PartitionedOperation<IN,OUT>
{
//  public OUT perform(StorableKey key, IN arg) throws InterruptedException;
  public OUT perform(Object key, IN arg) throws InterruptedException;

}
