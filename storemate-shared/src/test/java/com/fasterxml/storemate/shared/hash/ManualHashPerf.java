package com.fasterxml.storemate.shared.hash;

import java.io.*;
import java.util.zip.Adler32;

public class ManualHashPerf
{
    // Use field to 'return' value to avoid some dead code optimizations
    private int hash = 0;
    
    private void test(byte[] input) throws Exception
    {
        // Let's try to guestimate suitable size... to get to 50 megs to process
        final int REPS = (int) ((double) (50 * 1000 * 1000) / (double) input.length);

        System.out.println("Read "+input.length+" bytes to hash; will do "+REPS+" repetitions");

        int i = 0;
        
        while (true) {
            try {  Thread.sleep(100L); } catch (InterruptedException ie) { }
            int round = (i++ % 3);

            String msg;
            boolean lf = (round == 0);

            // Let's order this from slowest to fastest...
            
            final long start = System.currentTimeMillis();
            switch (round) {
            case 0:
                msg = "Adler32";
                hash = testMurmurBlock(REPS, input);
                break;
            case 1:
                msg = "Murmur3/Incremental";
                hash = testMurmurIncr(REPS, input);
                break;
            case 2:
                msg = "Murmur3/FULL";
                hash = testAdler32(REPS, input);
                break;
            default:
                throw new Error();
            }
            long msecs = System.currentTimeMillis() - start;
            
            if (lf) {
                System.out.println();
            }
            System.out.printf("Test '%s' [hash: 0x%s] -> %d msecs\n", msg, Integer.toHexString(this.hash), msecs);
        }
    }

    private final int testAdler32(int REPS, byte[] input) throws Exception
    {
        int result = 0;
        Adler32 adler = new Adler32();
        while (--REPS >= 0) {
        	adler.reset();
        	adler.update(input, 0, input.length);
            result = (int) adler.getValue();
        }
        return result;
    }

    private final int testMurmurIncr(int REPS, byte[] input) throws Exception
    {
        int result = 0;
        final IncrementalMurmur3Hasher hasher = new IncrementalMurmur3Hasher(0);
        while (--REPS >= 0) {
        	hasher.reset();
        	hasher.update(input, 0, input.length);
        	result = hasher.calculateHash();
        }
        return result;
    }

    private final int testMurmurBlock(int REPS, byte[] input) throws Exception
    {
        int result = 0;
        while (--REPS >= 0) {
            result = (int) BlockMurmur3Hasher.instance.hash(0, input, 0, input.length);
        }
        return result;
    }
    
    public static void main(String[] args) throws Exception
    {
        if (args.length != 1) {
            System.err.println("Usage: java ... [file]");
            System.exit(1);
        }
        File f = new File(args[0]);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream((int) f.length());
        byte[] buffer = new byte[4000];
        int count;
        FileInputStream in = new FileInputStream(f);
        
        while ((count = in.read(buffer)) > 0) {
            bytes.write(buffer, 0, count);
        }
        in.close();
        new ManualHashPerf().test(bytes.toByteArray());
    }

}
