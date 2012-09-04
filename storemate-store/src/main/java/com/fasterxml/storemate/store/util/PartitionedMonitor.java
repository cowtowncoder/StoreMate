package com.fasterxml.storemate.store.util;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.*;

/**
 * Helper class for doing N-way partitioned, thread-per-partition
 * FIFO monitor/mutex. Simple, as it just delegeates to a set of
 * {@link Semaphore}s to do the job.
 */
public class PartitionedMonitor
{
    private final static int MIN_PARTITIONS = 4;
    private final static int MAX_PARTITIONS = 1024;

    protected final int _modulo;

    /**
     * Underlying semaphores used for locking
     */
    protected final Semaphore[] _semaphores;

    /**
     * 
     * @param n Minimum number of partitions (rounded up to next power of 2)
     * @param fair Whether underlying semaphores should be fair or not; fair ones have
     *   more overhead, but mostly (only?) for contested access, not uncontested
     */
    public PartitionedMonitor(int n, boolean fair)
    {
        n = powerOf2(n);
        _modulo = n-1;
        _semaphores = new Semaphore[n];
        for (int i = 0; i < n; ++i) {
            _semaphores[i] = new Semaphore(1, true);
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

    public <IN,OUT> OUT perform(Object key, PartitionedOperation<IN,OUT> oper, IN arg)
            throws InterruptedException
    {
        final int partition = key.hashCode() & _modulo;
        final Semaphore semaphore = _semaphores[partition];
        semaphore.acquire();
        try {
            return oper.perform(key, arg);
        } finally {
            semaphore.release();
        }
    }

    public static void main(String[] args) throws Exception
    {
        /*
        final int THREADS = 8;
        final int COUNTERS = 4;
        */

        final int THREADS = 4;
        final int COUNTERS = 16;
        
        final int ROUNDS = 125000;

        final AtomicInteger running = new AtomicInteger(THREADS);
        
        final int[] sums = new int[COUNTERS];
        final Worker[] workers = new Worker[THREADS];
        final PartitionedMonitor monitor = new PartitionedMonitor(COUNTERS, true);
        
        for (int i = 0; i < THREADS; ++i) {
            workers[i] = new Worker(i, running, ROUNDS, sums, monitor);
        }
        final long start = System.currentTimeMillis();
        for (int i = 0; i < THREADS; ++i) {
            new Thread(workers[i]).start();
        }

        // wait for done...
        System.out.print("Threads running: ");
        while (true) {
            synchronized (monitor) {
                if (running.get() == 0) {
                    break;
                }
                monitor.wait();
            }
            System.out.printf(" %d", running.get());
        }
        System.out.println();
        System.out.print("Done, sums:");
        int total = 0;
        for (int i = 0; i < COUNTERS; ++i) {
            System.out.printf(" %s", Integer.toHexString(sums[i]));
            total += sums[i];
        }
        System.out.println(" for total of 0x"+Integer.toHexString(total));
        System.out.printf(" (and %d msecs)\n", System.currentTimeMillis()-start);
    }

    static class Worker implements Runnable
    {
        final PartitionedMonitor _monitor;
        final Random _rnd;
        final AtomicInteger _running;
        final int[] _counters;
        int _rounds;
        
        public Worker(int nr,
                AtomicInteger running, int rounds, int[] counters, PartitionedMonitor monitor)
        {
            _rnd = new Random(nr);
            _running = running;
            _rounds = rounds;
            _counters = counters;
            _monitor = monitor;
        }

        @Override
        public void run() {
            try {
                while (--_rounds >= 0) {
                    int next = _rnd.nextInt();
                    int part = (next & (_counters.length - 1));
                    withMonitor(part);
                }
            } catch (Exception e) {
                System.err.println("FAIL: "+e.getMessage());
                e.printStackTrace();
            }
            // done!
            _running.addAndGet(-1);
            synchronized (_monitor) {
                _monitor.notify();
            }
        }

        void withMonitor(final int part) throws InterruptedException
        {
            _monitor.perform(new IntKey(part), new PartitionedOperation<Object, Object>() {
                @Override
                public Object perform(Object key, Object arg)
                        throws InterruptedException {
                    // no syncing, monitor should guarantee it
                    int sum = _counters[part] + _calcSilly();
                    _counters[part] = sum;
                    return null;
                }
            }, null);
        }

        int _calcSilly()
        {
            int sum = 0;
            for (int i = 1; i < 20; ++i) {
                sum += _rnd.nextInt(i);
            }
            return sum;
        }

    }
    
    final static class IntKey {
        int num;
        
        public IntKey(int n) { num = n; }
        @Override public int hashCode() { return num; }
    }
    
}
