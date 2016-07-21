/**
 * MetadataSizeTest.java
 * Christopher Chute, David Brandfonbrener, Leo Shimonaka, Matt Vasseur
 * 
 * ConMixedTest.java
 * Measure HDFS throughput with high concurrency of mixed reads and writes.
 */

import java.io.File;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.Queue;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Random;

public class ConMixedTest {
    private static final boolean _DEBUG = false;
    private static final int MAX_NUM_THRUPUT_THREADS = 16;
    private static final int NUM_TRIALS = 10;
    private static final int NUM_THRUPUT_FILES = 500; // number files for xput test
    private static final double PROB_READ = 0.8;      // probability that  mixed test performs a read (1-p is prob. of write)
    private static final String LOCAL_READ_DIR = "/usr/local/hadoop/test/read";     // contains names to read from server
    private static final String HDFS_READ_DIR = "/read/";                           // HDFS path from which to read
    private static final String LOCAL_WRITE_DIR = "/usr/local/hadoop/test/tmp10B";  // contains files to randomly add to server
    private static final String HDFS_WRITE_DIR = "/throughput/";                    // HDFS path to which to write

    // prints out throughput by trial in CSV format
    public static void main(String[] args) {
        LinkedList<String> requestQ = new LinkedList<String>();
        Thread[] threadPool = new Thread[MAX_NUM_THRUPUT_THREADS];
        LinkedList<String> filesAdded = new LinkedList<String>();
        double NUM_THRUPUT_THREADS; // how many threads to run the test on

        // run NUM_TRIALS trials, adding more threads sequentially
        for (int i = 1; i <= NUM_TRIALS; ++i) {
            NUM_THRUPUT_THREADS = Math.pow(2,i-1); // increase threads each trial

            System.out.print(NUM_THRUPUT_THREADS + ",");
            // STEP 1: throughput measurement test
            System.err.println("(1) Mixed Read/Write Throughput (trial " +
            ((i-1)) + ")");
            System.err.println("(1a) filling mixed read/write request queue");
            for (int j = 1; j <= NUM_THRUPUT_FILES; ++j) {
                File readFile;
                do {
                    readFile = getRandomFile(LOCAL_READ_DIR);
                } while (filesAdded.contains(readFile.getName()));
                filesAdded.offer(readFile.getName());

                // mix reads and writes (reads with .8 probability)
                String request;
                if (Math.random() < PROB_READ) {
                    request = new String("read " + HDFS_READ_DIR + readFile.getName());
                } else {
                    request = new String("add " + "./read/" + readFile.getName() +
                    " " + HDFS_WRITE_DIR + readFile.getName());
                }

                synchronized (requestQ) {
                    requestQ.offer(request);
                    requestQ.notifyAll();
                }
            }

            // start thread pool to carry out mixed read/writ requests
            for (int j = 0; j < NUM_THRUPUT_THREADS; ++j) {
                threadPool[j] = new Thread(new HDFSClient(requestQ));
                threadPool[j].start();
            }
            Long startTime = System.currentTimeMillis();

            for (int j = 0; j < NUM_THRUPUT_THREADS; ++j) {
                try {
                    threadPool[j].join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
            // write out throughput
            Long endTime = System.currentTimeMillis();
            Double totalTime = 1.0 * (endTime - startTime) / 1000;
            System.out.println(String.format("%.4f", 1.0 * (NUM_THRUPUT_FILES / totalTime)));

            // clean up the writes
            synchronized (requestQ) {
                while(!filesAdded.isEmpty()) {
                    String fileName = filesAdded.poll();
                    File fileToDelete = new File("./" + fileName);
                    fileToDelete.delete();
                    requestQ.offer("delete " + HDFS_WRITE_DIR + fileName);
                }
            }
            // wait for threads to clean up HDFS
            for (int j = 0; j < MAX_NUM_THRUPUT_THREADS; ++j) {
                threadPool[j] = new Thread(new HDFSClient(requestQ));
                threadPool[j].start();
            }
            for (int j = 0; j < MAX_NUM_THRUPUT_THREADS; ++j) {
                try {
                    threadPool[j].join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }

            // wait for use input to synchronized each trial start across nodes
            System.out.println("Ending trial " + i + ", waiting for user input");
            try {
                // wait for user to hit enter
                System.in.read(new byte[2]);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } // END TRIAL LOOP

        return;
    }

    // get a random file from a specified directory
    private static File getRandomFile(final String dir) {
        File folder = new File(dir);
        Random rand = new Random();

        File[] files = folder.listFiles();

        return files[rand.nextInt(files.length)];
    }

    private static void DEBUG(String str) {
        if (_DEBUG) {
            System.err.println(str);
        }
    }
}
