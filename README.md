# HDFS Scalability Limits
This paper explores the scalability limits of the [Hadoop Distributed File System](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (HDFS). The single-machine, in-memory metadata store of HDFS is a known bottleneck for applications requiring storage of many small files.
In addition, the singular NameNode presents possible CPU bottlenecks when scaling for storage of a large number of small files.
For example, small files create more complex heartbeats and are associated with more frequent access request patterns, both of which increase the CPU load on the NameNode.
In this paper we first model and provide experimental evidence for the NameNode memory bottleneck. We compare these results to proposed CPU bottlenecks, and we find that the memory bottleneck dominates in all reasonable usage patterns. We conclude with suggestions for future work on HDFS scalability limits.

### Test Client Design
We query HDFS using the [Hadoop Java API](http://hadoop.apache.org/docs/r2.7.2/api/). On top of the file system class provided by the API, we implemented a class called ```HdfsClient```. Each ```HdfsClient``` instance is ```Runnable```, and represents a single client making file access or modification requests to the file system. On each machine querying the NameNode, we spin up a pool of threads, each of which wraps an ```HdfsClient``` and reads from a global thread-safe request queue. The master thread (different in each experiment) generates requests and places them on the request queue for processing by the ```HdfsClient``` worker threads. Each client repeatedly removes a request from the queue, issues that request to the NameNode and waits for the subsequent response. Examples of requests include writing a file from the local file system to HDFS, reading a file from HDFS, and deleting a file from HDFS.

### Authors
Christopher Chute, David Brandfonbrener, Leo Shimonaka, and Matthew Vasseur