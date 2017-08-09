## Description

This application demonstrates continuous big data archival from HDFS.
It ingests files as blocks for backup on remote HDFS cluster.

- Applications avoids overhead of re-constructing the original file. Thus, it can process blocks in parallel.
The application keeps the same data at destination but creates part files instead of complete file.
- The application scales linearly with number of block readers.
- The application is fault tolerant and can withstand for node and cluster outages without data loss.
- The application is also highly performant and can perform equivalent to network speed.
- The only configuration user needs to provide is source and destination HDFS paths.
- This enterprise grade application template will dramatically reduce your time to market and cost of operations.

## Implementation details

- Logical flow diagram

   ![Logical Plan](https://www.datatorrent.com/wp-content/uploads/2017/06/HDFS_Part_File_Copy_DAG.png)
- It uses following operators
  - FSInputModule
  - PartFileWriter
- Supported data source
  - Apache Hadoop HDFS
  - Tested with hadoop library: org.apache.hadoop:hadoop-common:jar:2.6.0
- Supported sinks
  - Apache Hadoop HDFS
  - Tested with hadoop library: org.apache.hadoop:hadoop-common:jar:2.6.0

## Resources

- Detailed documentation for this app-template is available at:

   <a
     href="http://docs.datatorrent.com/app-templates/0.10.0/hdfs-part-file-copy/"  class="docs" id="docs" ga-track="docs"
     target="_blank">http://docs.datatorrent.com/app-templates/0.10.0/hdfs-part-file-copy/</a>
- Source code for this app-template is available at :

    <a
     href="https://github.com/DataTorrent/moodI/tree/master/app-templates/hdfs-part-file-copy"  class="github" id="github" ga-track="github" target="_blank">https://github.com/DataTorrent/moodI/tree/master/app-templates/hdfs-part-file-copy</a>

- Please send feedback or feature requests to:
    <a href="mailto:feedback@datatorrent.com"  class="feedback" id="feedback" ga-track="feedback">feedback@datatorrent.com</a>

- Join our user discussion group at:
    <a href="mailto:dt-users@googlegroups.com"  class="maillist" id="maillist" ga-track="maillist">dt-users@googlegroups.com</a>
