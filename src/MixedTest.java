/**
 * MixedTest.java
 * Christopher Chute, David Brandfonbrener, Leo Shimonaka, Matt Vasseur
 * 
 * Measure throughput of mixed reads and writes as the number of small
 * files on HDFS varies.
 */

import java.io.File;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.Queue;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Random;

public class MixedTest {
    private static final boolean _DEBUG = false;
    private static final int NUM_WRITE_THREADS = 16;
    private static final int NUM_THRUPUT_THREADS = 8;
    private static final int NUM_TRIALS = 99;
    private static final int NUM_WRITES = 1000;
    private static final int NUM_THRUPUT_FILES = 500; // number files for xput test
    private static final double PROB_READ = 0.8;      // probability that the mixed test performs a read (0.2 is prob is makes a write)
    private static final String LOCAL_READ_DIR = "/usr/local/hadoop/test/read";     // contains names to read from server
    private static final String HDFS_READ_DIR = "/read/";                           // HDFS path from which to read
    private static final String LOCAL_WRITE_DIR = "/usr/local/hadoop/test/tmp10B";  // contains files to randomly add to server
    private static final String HDFS_WRITE_DIR = "/throughput/";                    // HDFS path to which to write

    public static void main(String[] args) {
        LinkedList<String> requestQ = new LinkedList<String>();
        Thread[] threadPool = new Thread[NUM_WRITE_THREADS];
        LinkedList<String> filesAdded = new LinkedList<String>();

        // run NUM_TRIALS trials, adding more files sequentially
        for (int i = 1; i <= NUM_TRIALS; ++i) {
            System.out.print((i - 1) * NUM_WRITES + ",");
            // STEP 1: throughput measurement test
            System.err.println("(1) Mixed Read/Write Throughput (" +
            ((i-1)*NUM_WRITES) + " extra files on HDFS)");
            System.err.println("(1a) filling mixed read/write request queue");
            for (int j = 1; j <= NUM_THRUPUT_FILES; ++j) {
                File readFile;
                do {
                    readFile = getRandomFile(LOCAL_READ_DIR);
                } while (filesAdded.contains(readFile.getName()));
                filesAdded.offer(readFile.getName());

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
                threadPool[j] = new Thread(new HdfsClient(requestQ));
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
            for (int j = 0; j < NUM_THRUPUT_THREADS; ++j) {
                threadPool[j] = new Thread(new HdfsClient(requestQ));
                threadPool[j].start();
            }
            for (int j = 0; j < NUM_THRUPUT_THREADS; ++j) {
                try {
                    threadPool[j].join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }

            System.err.println("(2) Adding " + NUM_WRITES + " files");
            System.err.println("(2a) putting write requests in queue");
            for (int j = 1; j <= NUM_WRITES; ++j) {
                File writeFile = getRandomFile(LOCAL_WRITE_DIR);

                StringBuilder requestBuf = new StringBuilder();
                requestBuf.append("add ");
                requestBuf.append(writeFile.getAbsolutePath());
                requestBuf.append(" " + HDFS_WRITE_DIR +
                String.format("%02d", i) + String.format("%05d", j)  + writeFile.getName());
                // places on HDFS with appended "ij" for unique ID
                synchronized (requestQ) {
                    DEBUG("adding request to requestQ");
                    if (j % 1000 == 0) {
                        requestQ.offer("DONE " + j);
                    }
                    requestQ.offer(requestBuf.toString());
                    requestQ.notifyAll();
                }
            }
            System.err.println("Done adding requests to queue");
            for (int j = 0; j < NUM_WRITE_THREADS; ++j) {
                threadPool[j] = new Thread(new HdfsClient(requestQ));
                threadPool[j].start();
            }
            // wait for all threads to finish, then measure throughput
            for (int j = 0; j < NUM_WRITE_THREADS; ++j) {
                try {
                    threadPool[j].join();
                } catch (InterruptedException ex) {

                }
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
