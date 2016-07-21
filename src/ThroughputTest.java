/**
 * ThroughputTest.java
 * Christopher Chute, David Brandfonbrener, Leo Shimonaka, Matt Vasseur
 * 
 * Measure the throughput of reads and writes separately as the number
 * of small files on HDFS varies.
 */

import java.io.File;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.Queue;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Random;

public class ThroughputTest {
    private static final boolean _DEBUG = false;
    private static final int NUM_WRITE_THREADS = 16;
    private static final int NUM_THRUPUT_THREADS = 16;
    private static final int NUM_TRIALS = 99;
    private static final int NUM_WRITES = 1000;
    private static final int NUM_THRUPUT_FILES = 500;    // number files for xput test
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
            System.err.println("(1) Read Throughput (" +
            ((i-1)*NUM_WRITES) + " extra files on HDFS)");
            System.err.println("(1a) filling read request queue");
            for (int j = 1; j <= NUM_THRUPUT_FILES; ++j) {
                File readFile;
                do {
                    readFile = getRandomFile(LOCAL_READ_DIR);
                } while (filesAdded.contains(readFile.getName()));
                filesAdded.offer(readFile.getName());

                StringBuilder requestBuf = new StringBuilder();
                requestBuf.append("read ");
                requestBuf.append(HDFS_READ_DIR + readFile.getName());

                synchronized (requestQ) {
                    requestQ.offer(requestBuf.toString());
                    requestQ.notifyAll();
                }
            }

            // start thread pool to carry out read requests
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
            // write out read throughput
            Long endTime = System.currentTimeMillis();
            Double totalTime = 1.0 * (endTime - startTime) / 1000;
            System.out.print(String.format("%.4f", 1.0 * (NUM_THRUPUT_FILES / totalTime)) + ",");


            // STEP 2: measure write throughput
            System.err.println("(2) Write Throughput (" +
            ((i-1)*NUM_WRITES) + " extra files on HDFS)");
            System.err.println("(2a) filling write request queue");
            for (String fileName : filesAdded) {
                String request = new String("add " +  "./" + fileName +
                " " + HDFS_WRITE_DIR + fileName);
                synchronized (requestQ) {
                    requestQ.offer(request);
                    requestQ.notifyAll();
                }
            }

            System.err.println("(2b) starting throughput measurement");
            // start thread pool to carry out read requests
            for (int j = 0; j < NUM_THRUPUT_THREADS; ++j) {
                threadPool[j] = new Thread(new HdfsClient(requestQ));
                threadPool[j].start();
            }
            startTime = System.currentTimeMillis();

            for (int j = 0; j < NUM_THRUPUT_THREADS; ++j) {
                try {
                    threadPool[j].join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
            // write out write throughput
            endTime = System.currentTimeMillis();
            totalTime = 1.0 * (endTime - startTime) / 1000;
            System.out.println(String.format("%.4f", 1.0 * (NUM_THRUPUT_FILES / totalTime)));

            // clean up the writes and local reads
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

            System.err.println("(3) Adding " + NUM_WRITES + " files");
            System.err.println("(3a) putting write requests in queue");
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
