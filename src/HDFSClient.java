/**
 * HdfsClient.java
 * Christopher Chute, David Brandfonbrener, Leo Shimonaka, Matt Vasseur
 * 
 * Base client for create, read, update, delete operations to HDFS. 
 * Used in all throughput tests.
 * REFERENCE: Adapted from http://tinyurl.com/hdfs-java-api
 */

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Queue;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class HdfsClient implements Runnable {
    private Queue<String> requestQ;
    private volatile boolean isStopped;

    public HdfsClient(final Queue<String> requestQ) {
        this.requestQ = requestQ;
        isStopped = false;
    }

    /* run: repeatedly grab a command-line job off the requestQ and process */
    public void run() {
        try {  // TODO: catching IOException over whole function
            while (!isStopped) {
                String request = null;
                synchronized (requestQ) {
                    // CHANGED for MetadataSizeTest vs. others (while vs. if)
                    if (requestQ.isEmpty()) {
                        System.err.println("Terminating " + this);
                        isStopped = true;
                        continue;
                        /*
                        use this for MetadataSizeTest
                        try {
                            requestQ.wait();
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                        */
                    }
                    request = requestQ.poll();
                }
                if (request == null) {
                    continue;
                } else if (request.charAt(0) == 'D' && request.charAt(1) == 'O') {
                    // request is DONE so print that (for debugging/timing)
                    System.err.println(request);
                    continue;
                }
                String[] requestArgs = request.split(" ", 3);

                if (requestArgs.length < 2) {
                    return;
                }

                if (requestArgs[0].equals("add")) {

                    addFile(requestArgs[1], requestArgs[2]);

                } else if (requestArgs[0].equals("read")) {

                    readFile(requestArgs[1]);

                } else if (requestArgs[0].equals("delete")) {

                    deleteFile(requestArgs[1]);

                } else if (requestArgs[0].equals("mkdir")) {

                    mkdir(requestArgs[1]);

                } else if (requestArgs[0].equals("copyfromlocal")) {

                    copyFromLocal(requestArgs[1], requestArgs[2]);

                } else if (requestArgs[0].equals("rename")) {

                    renameFile(requestArgs[1], requestArgs[2]);

                } else if (requestArgs[0].equals("copytolocal")) {

                    copyToLocal(requestArgs[1], requestArgs[2]);

                } else if (requestArgs[0].equals("modificationtime")) {

                    getModificationTime(requestArgs[1]);

                } else if (requestArgs[0].equals("getblocklocations")) {

                    getBlockLocations(requestArgs[1]);

                } else if (requestArgs[0].equals("gethostnames")) {

                    getHostnames();

                } else {
                    printUsage();
                    System.exit(1);
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /* stopThread: gracefully shutdown the client thread */
    public void stopThread() {
        isStopped = true;
    }

    public static void printUsage(){
        System.out.println("Usage: hdfsclient add" + "<local_path> <hdfs_path>");
        System.out.println("Usage: hdfsclient read" + "<hdfs_path>");
        System.out.println("Usage: hdfsclient delete" + "<hdfs_path>");
        System.out.println("Usage: hdfsclient mkdir" + "<hdfs_path>");
        System.out.println("Usage: hdfsclient copyfromlocal" + "<local_path> <hdfs_path>");
        System.out.println("Usage: hdfsclient copytolocal" + " <hdfs_path> <local_path> ");
        System.out.println("Usage: hdfsclient modificationtime" + "<hdfs_path>");
        System.out.println("Usage: hdfsclient getblocklocations" + "<hdfs_path>");
        System.out.println("Usage: hdfsclient gethostnames");
    }

    public boolean ifExists (Path source) throws IOException {

        Configuration config = new Configuration();
        config.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        config.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        config.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

        FileSystem hdfs = FileSystem.newInstance(config);
        boolean isExists = hdfs.exists(source);
        return isExists;
    }

    public void getHostnames() throws IOException{
        Configuration config = new Configuration();
        config.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        config.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        config.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

        FileSystem fs = FileSystem.newInstance(config);
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

        String[] names = new String[dataNodeStats.length];
        for (int i = 0; i < dataNodeStats.length; i++) {
            names[i] = dataNodeStats[i].getHostName();
            System.out.println((dataNodeStats[i].getHostName()));
        }
    }

    public void getBlockLocations(String source) throws IOException{

        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

        FileSystem fileSystem = FileSystem.newInstance(conf);
        Path srcPath = new Path(source);

        // Check if the file already exists
        if (!(ifExists(srcPath))) {
            System.out.println("No such destination " + srcPath);
            return;
        }
        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        FileStatus fileStatus = fileSystem.getFileStatus(srcPath);

        BlockLocation[] blkLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        int blkCount = blkLocations.length;
    }

    public void getModificationTime(String source) throws IOException{

        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

        FileSystem fileSystem = FileSystem.newInstance(conf);
        Path srcPath = new Path(source);

        // Check if the file already exists
        if (!(fileSystem.exists(srcPath))) {
            System.out.println("No such destination " + srcPath);
            return;
        }
        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        FileStatus fileStatus = fileSystem.getFileStatus(srcPath);
        long modificationTime = fileStatus.getModificationTime();

        System.out.format("File %s; Modification time : %0.2f %n",filename,modificationTime);

    }

    public void copyFromLocal (String source, String dest) throws IOException {

        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

        FileSystem fileSystem = FileSystem.newInstance(conf);
        Path srcPath = new Path(source);

        Path dstPath = new Path(dest);
        // Check if the file already exists
        if (!(fileSystem.exists(dstPath))) {
            System.out.println("No such destination " + dstPath);
            return;
        }

        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        try{
            fileSystem.copyFromLocalFile(srcPath, dstPath);
            System.out.println("File " + filename + "copied to " + dest);
        }catch(Exception e){
            System.err.println("Exception caught! :" + e);
            System.exit(1);
        }finally{
            fileSystem.close();
        }
    }

    public void copyToLocal (String source, String dest) throws IOException {

        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

        FileSystem fileSystem = FileSystem.newInstance(conf);
        Path srcPath = new Path(source);

        Path dstPath = new Path(dest);
        // Check if the file already exists
        if (!(fileSystem.exists(srcPath))) {
            System.out.println("No such destination " + srcPath);
            return;
        }

        // Get the filename out of the file path
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

        try{
            fileSystem.copyToLocalFile(srcPath, dstPath);
            System.out.println("File " + filename + "copied to " + dest);
        }catch(Exception e){
            System.err.println("Exception caught! :" + e);
            System.exit(1);
        }finally{
            fileSystem.close();
        }
    }

    public void renameFile (String fromthis, String tothis) throws IOException{
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

        FileSystem fileSystem = FileSystem.newInstance(conf);
        Path fromPath = new Path(fromthis);
        Path toPath = new Path(tothis);

        if (!(fileSystem.exists(fromPath))) {
            System.out.println("No such destination " + fromPath);
            return;
        }

        if (fileSystem.exists(toPath)) {
            System.out.println("Already exists! " + toPath);
            return;
        }

        try{
            boolean isRenamed = fileSystem.rename(fromPath, toPath);
            if(isRenamed){
                System.out.println("Renamed from " + fromthis + "to " + tothis);
            }
        }catch(Exception e){
            System.out.println("Exception :" + e);
            System.exit(1);
        }finally{
            fileSystem.close();
        }

    }

    public void addFile(String source, String dest) throws IOException {

        // Conf object will read the HDFS configuration parameters
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

        FileSystem fileSystem = FileSystem.newInstance(conf);

        // CHANGED: Get the filename out of the file path
        if (dest.charAt(dest.length() - 1) == '/') {
            String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
            dest += filename;
        }

        // Check if the file already exists
        Path path = new Path(dest);
        if (fileSystem.exists(path)) {
            System.err.println("File " + dest + " already exists");
            return;
        }

        // Create a new file and write data to it.
        FSDataOutputStream out = fileSystem.create(path);
        InputStream in = new BufferedInputStream(new FileInputStream(
        new File(source)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        // Close all the file descripters
        in.close();
        out.close();
        fileSystem.close();
    }

    public void readFile(String file) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

        FileSystem fileSystem = FileSystem.newInstance(conf);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exist");
            return;
        }

        FSDataInputStream in = fileSystem.open(path);

        String filename = file.substring(file.lastIndexOf('/') + 1,
        file.length());

        OutputStream out = new BufferedOutputStream(new FileOutputStream(
        new File(filename)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
        fileSystem.close();
    }

    public void deleteFile(String file) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

        FileSystem fileSystem = FileSystem.newInstance(conf);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.err.println("File " + file + " does not exists");
            return;
        }

        fileSystem.delete(new Path(file), true);

        fileSystem.close();
    }

    public void mkdir(String dir) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/mapred-site.xml"));

        FileSystem fileSystem = FileSystem.newInstance(conf);

        Path path = new Path(dir);
        if (fileSystem.exists(path)) {
            System.out.println("Dir " + dir + " already exists!");
            return;
        }

        fileSystem.mkdirs(path);

        fileSystem.close();
    }
}
