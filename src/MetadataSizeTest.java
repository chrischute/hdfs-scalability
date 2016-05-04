/* Christopher Chute, David Brandfonbrener, Leo Shimonaka, Matt Vasseur */
/* CPSC 433 - Databases */
/* May 2, 2016 */

import java.io.File;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Random;

public class MetadataSizeTest {
    private static final boolean _DEBUG = false;
    private static final int PRINT_INTERVAL = 100;
    private static final int NUM_CLIENT_THREADS = 32;
    private static final int NUM_FILES = 5000;
    private static final int NUM_TRIALS = 99;
    private static final String LOCAL_FILE_DIR = "/usr/local/hadoop/test/tmp10B";
    private static final String HDFS_FILE_DIR = "/bigmdst/";

    public static void main(String[] args) {
        LinkedList<String> requestQ = new LinkedList<String>();
        Thread[] threadPool = new Thread[NUM_CLIENT_THREADS];

        // spin up HDFSClient threads
        for (int j = 0; j < NUM_CLIENT_THREADS; ++j) {
            threadPool[j] = new Thread(new HDFSClient(requestQ));
            threadPool[j].start();
        }

        // run NUM_TRIALS trials, adding more files sequentially
        for (int i = 1; i <= NUM_TRIALS; ++i) {
            System.out.println("Starting trial number " + i);
            for (int j = 1; j <= NUM_FILES; ++j) {
                File fileToAdd = getRandomFile(LOCAL_FILE_DIR);

                // add files requests so that they do not conflict
                StringBuilder requestBuf = new StringBuilder();
                requestBuf.append("add ");
                requestBuf.append(fileToAdd.getAbsolutePath());
                requestBuf.append(" " + HDFS_FILE_DIR +
                   String.format("%02d", i) + String.format("%05d", j)  + fileToAdd.getName());
                // places on HDFS with appended "ij" for unique ID
                synchronized (requestQ) {
                    DEBUG("adding request to requestQ");
                    requestQ.offer(requestBuf.toString());

                    if (j % PRINT_INTERVAL == 0) {
                        requestQ.offer("DONE " + (j + (i - 1) * NUM_FILES));
                    }
                    requestQ.notifyAll();
                }
            }

            // wait for all threads to finish, then restart on user input
            System.out.println("Ending trial " + i + ", waiting for user input");
            try {
                // wait for user to hit enter
                System.in.read(new byte[2]);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        for (int j = 0; j < NUM_CLIENT_THREADS; ++j) {
            try {
                threadPool[j].join();
            } catch (InterruptedException ex) {

            }
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
