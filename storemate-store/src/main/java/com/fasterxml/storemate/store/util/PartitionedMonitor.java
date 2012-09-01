package com.fasterxml.storemate.store.util;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Helper class for doing N-way partitioned, thread-per-partition
 * FIFO monitor/mutex.
 */
public class PartitionedMonitor
{
	private final static int MIN_PARTITIONS = 4;
	private final static int MAX_PARTITIONS = 1024;
	
	protected final int _modulo;
	
	/**
	 * Set of simple per-partition thread counters.
	 */
	protected final AtomicIntegerArray _counters;

	protected final Semaphore[] _semaphores;
	
	public PartitionedMonitor(int n)
	{
		n = powerOf2(n);
		_counters = new AtomicIntegerArray(n);
		_modulo = n-1;
		_semaphores = new Semaphore[n];
		for (int i = 0; i < n; ++i) {
			// use "fair" semaphores, since we can mostly avoid them altogether
			_semaphores[i] = new Semaphore(0, true);
		}
	}

	private final static int powerOf2(int n)
	{
		if (n <= MIN_PARTITIONS) return MIN_PARTITIONS;
		if (n >= MAX_PARTITIONS) return MAX_PARTITIONS;
		int m = MIN_PARTITIONS;
		while (m < n) {
			m += m;
		}
		return m;
	}

    public <IN,OUT> OUT perform(StorableKey key, Operation<IN,OUT> oper, IN arg)
    	throws InterruptedException
	{
    	int partition = key.hashCode() & _modulo;

    	/* Need to acquire a semaphore? Only if we are NOT the only one trying
    	 * to do that...
    	 * (why two parts? becomes first call should mostly succeed, and is
    	 * faster+simpler than the second call)
    	 */
    	if (!_counters.compareAndSet(partition, 0, 1)
    			&& _counters.getAndAdd(partition, 1) > 0) {
    		_semaphores[partition].acquire();
    	}
		try {
			return oper.perform(key, arg);
		} finally {
			/* Conversely, only need to release semaphore if there is someone
			 * still waiting.
			 */
			if (!_counters.compareAndSet(partition, 1, 0)
					&& _counters.addAndGet(partition, -1) > 0) {
				_semaphores[partition].release();
			}
		}
	}
	
	public interface Operation<IN,OUT> {
		public OUT perform(StorableKey key, IN arg) throws InterruptedException;
	}

}
