## Description

The Amazon S3 to HDFS sync application template demonstrates continuous big data sync from a source to destination while reading blocks of data from a configured S3 bucket. This could be easily utilized and extended by any developer to create a fast,  fault tolerant and scalable Big Data Sync or Retention Application to serve business with continuous data.

It continuously ingests files as blocks from the configured Amazon S3 location to the destination Hadoop cluster (path in HDFS) while retaining one-to-one file traceability.

- The application scales linearly with number of block readers.
- The application is fault tolerant and can withstand node and cluster outages without data loss.
- The application is also highly performant and can perform as fast as the network bandwidth allows.
- The only configuration user needs to provide is source S3 connection parameters, bucket, directory and destination HDFS path, filename.
- This enterprise grade application template will dramatically reduce your time to market and cost of operations

Import the application from DataTorrent AppFactory and launch it to ingest your files from Amazon S3 to HDFS. Please follow the walkthrough document below to understand the configurable properties. Moreover, one could easily add customized business logic to further process the data during sync.

## Implementation details

- Logical flow diagram
    ![Logical Plan](http://datatorrent.com/wp-content/uploads/2016/11/S3_HDFS_Sync_App_DAG.png)

- It uses following operators
  - S3InputModule Input Operator (Read from source Amazon S3 service)
  - HDFSFileCopyModule Output Operator (Write to Hadoop Cluster)
- Supported data source
    - AWS S3 version 1.10.x
    - Tested with AWS Redshift JDBC driver: com.amazonaws:aws-java-sdk-s3:jar:1.10.73
- Supported sinks
  - Apache Hadoop HDFS version 2.6.x
  - Tested with hadoop library: org.apache.hadoop:hadoop-common:jar:2.6.0

## Supported visualizations
|Metric|Widget|
|------|------|
|Discovered file count| Single Value|
|Complted file count| Single Value|
|Bytes read per second from S3 |Line Chart|
|Bytes written per second to HDFS |Line Chart|
|Total bytes read from S3 |Single Value|
|Total bytes written to HDFS |Single Value|

## Resources

- Detailed documentation for this application template is available at :
[http://docs.datatorrent.com/app-templates/0.10.0/s3-to-hdfs-sync](http://docs.datatorrent.com/app-templates/0.10.0/s3-to-hdfs-sync)

- Source code for this app-template is available at :
[https://github.com/DataTorrent/moodI/tree/master/app-templates/s3-to-hdfs-sync](https://github.com/DataTorrent/moodI/tree/master/app-templates/s3-to-hdfs-sync)

- Please send feedback or feature requests to :
    <a href="mailto:feedback@datatorrent.com"  class="feedback" id="feedback" ga-track="feedback">feedback@datatorrent.com</a>

- Join our user discussion group at :
    <a href="mailto:dt-users@googlegroups.com"  class="maillist" id="maillist" ga-track="maillist">dt-users@googlegroups.com</a>
