## Description

This application demonstrates data preparation pipeline which reads lines from files on source HDFS. It can perform customized filtering, transformations on the line and writes them into destination HDFS.

- The application scales linearly with number of record readers.
- The application is fault tolerant and can withstand node and cluster outages without data loss.
- The application is also highly performant and can perform as fast as the network allows.
- It is extremely easy to add custom logic to get your business value without worrying about connectivity and operational details of HDFS reader or writer.
- The only configuration user needs to provide is source and destination HDFS paths.
- This enterprise grade application template will dramatically reduce your time to market and cost of operations.

## Implementation details

- Logical flow diagram

   ![Logical Plan](http://datatorrent.com/wp-content/uploads/2016/11/HDFS_HDFS_Line_Copy_DAG.png)
- It uses following operators
  - FSRecordReaderModule
  - CsvParser
  - CsvFormatter
  - StringFileOutputOperator
- Supported data source
  - Apache Hadoop HDFS
  - Tested with hadoop library: org.apache.hadoop:hadoop-common:jar:2.6.0
- Supported sinks
  - Apache Hadoop HDFS
  - Tested with hadoop library: org.apache.hadoop:hadoop-common:jar:2.6.0

## Supported visualizations

  | Description  | Widget   |
  |---|---|
  | Bytes read per minute from source HDFS  | Line chart|
  | Bytes written per minute to destination HDFS | Line chart |
  | Events read per minute from source HDFS  | Line chart|
  | Events written per minute to destination HDFS | Line chart |

## Resources

  - Detailed documentation for this app-template is available at :

     <a
       href="http://docs.datatorrent.com/app-templates/0.10.0/hdfs-line-copy/"  class="docs" id="docs" ga-track="docs"
       target="_blank">http://docs.datatorrent.com/app-templates/0.10.0/hdfs-line-copy/</a>
  - Source code for this app-template is available at :

      <a
       href="https://github.com/DataTorrent/moodI/tree/master/app-templates/hdfs-line-copy"  class="github" id="github" ga-track="github" target="_blank">https://github.com/DataTorrent/moodI/tree/master/app-templates/hdfs-line-copy</a>

  - Please send feedback or feature requests to :
      <a href="mailto:feedback@datatorrent.com"  class="feedback" id="feedback" ga-track="feedback">feedback@datatorrent.com</a>

  - Join our user discussion group at :
      <a href="mailto:dt-users@googlegroups.com"  class="maillist" id="maillist" ga-track="maillist">dt-users@googlegroups.com</a>
