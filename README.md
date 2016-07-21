# HDFS Scalability Limits
This paper explores the scalability limits of the [Hadoop Distributed File System](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (HDFS). The single-machine, in-memory metadata store of HDFS is a known bottleneck for applications requiring storage of many small files.
In addition, the singular NameNode presents possible CPU bottlenecks when scaling for storage of a large number of small files.
For example, small files create more complex heartbeats and are associated with more frequent access request patterns, both of which increase the CPU load on the NameNode.
In this paper we first model and provide experimental evidence for the NameNode memory bottleneck. We compare these results to proposed CPU bottlenecks, and we find that the memory bottleneck dominates in all reasonable usage patterns. We conclude with suggestions for future work on HDFS scalability limits.

### Authors
Christopher Chute, David Brandfonbrener, Leo Shimonaka, and Matthew Vasseur
