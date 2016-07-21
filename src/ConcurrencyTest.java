/**
 * ConcurrencyTest.java
 * Christopher Chute, David Brandfonbrener, Leo Shimonaka, Matt Vasseur
 *
 * Measure HDFS throughput via calls to getBlockLocations.
 * Measured on load of many small files.
 */

import java.io.File;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.Queue;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Random;

public class ConcurrencyTest {
    private static final boolean _DEBUG = false;
    private static final int NUM_THRUPUT_THREADS = 16;
    private static final int NUM_TRIALS = 15;
    private static final int NUM_THRUPUT_FILES = 500;                           // number files for xput test
    private static final String LOCAL_READ_DIR = "/usr/local/hadoop/test/read"; // contains names to read from server
    private static final String HDFS_READ_DIR = "/read/";                       // HDFS path from which to read

    // prints out throughput by trial in CSV format
    public static void main(String[] args) {
        LinkedList<String> requestQ = new LinkedList<String>();
        Thread[] threadPool = new Thread[NUM_THRUPUT_THREADS];

        // run NUM_TRIALS trials
        for (int i = 1; i <= NUM_TRIALS; ++i) {
            System.out.print(i + ",");
            // test throughput via the getBlockLocations call
            System.err.println("(1) BlockSize Throughput (trial " + i + ")");
            System.err.println("(1a) filling read request queue");
            for (int j = 1; j <= NUM_THRUPUT_FILES; ++j) {
                File readFile;
                readFile = getRandomFile(LOCAL_READ_DIR);

                StringBuilder requestBuf = new StringBuilder();
                requestBuf.append("getblocklocations ");
                requestBuf.append(HDFS_READ_DIR + readFile.getName());

                synchronized (requestQ) {
                    requestQ.offer(requestBuf.toString());
                    requestQ.notifyAll();
                }
            }

            System.err.println("(1b) starting throughput measurement");
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
            // write out throughput
            Long endTime = System.currentTimeMillis();
            Double totalTime = 1.0 * (endTime - startTime) / 1000;
            System.out.println(String.format("%.4f", 1.0 * (NUM_THRUPUT_FILES / totalTime)));
        }

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
