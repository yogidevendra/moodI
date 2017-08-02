# Kafka to HDFS with filter transform application

## Description

This application demonstrates continuous ingestion of streaming data source into the big data lake. Application uses Kafka as a streaming source and HDFS as big data lake destination. Depending on the context, data in kafka could be coming from logs, sensors, call data records, transactions etc.

- The application supports configuring custom schema at the input. This makes it reusable across variety of business usecases.
- The application supports defining cusom filter criteria, transform expressions. This is useful if it is used as data preparation pipeline.
- The application supports visualization dashboard for real-time, historical metrics which can be used for monitoring. It can also be customized to add new metrics.
- The application scales linearly with the number of Kafka brokers and Kafka topics.
- The application is fault tolerant and can withstand node and cluster outages without data loss.
- The application is also highly performant and can process faster than Kafka broker can produce per topic.
- It is extremely easy to add custom logic to get your business value without worrying about connectivity and operational details of Kafka consumer and HDFS writer.
- User needs to provide configuration about Kafka source, input schema, filter conditions, transform expressions and destination HDFS path, filename to launch this application.

## Implementation details

- Logical flow diagram
   ![Logical Plan](https://logical-dag.png)
- It uses following operators
  - Kafka single port input operator
  - JSON parser
  - Transform operator
  - Filter operator
  - JSON formatter
  - String file output operator
- Supported data source
  - Apache Kafka version 0.9
  - Tested with kafka client library: org.apache.kafka:kafka_2.11:0.9.0.1
- Supported sinks
  - Apache Hadoop HDFS
  - Tested with hadoop library: org.apache.hadoop:hadoop-common:jar:2.6.0

## Supported visualizations

| Description  | Widget   |
|---|---|---|---|---|
| Bytes read per minute from Kafka  | Line chart|
| Bytes written per minute to HDFS | Line chart |
| Events read per minute from Kafka | Line chart |
| Event read per minute to HDFS  | Line chart  |
| Number of bad events per minute | Line chart  |
| Total number of bad events | Line chart  |

## Introductory video

<iframe src="https://www.youtube.com/watch?v=taHc_QJUfBg?enablejsapi=1" allowfullscreen="allowfullscreen" class="video" id="basicVideo" ga-track="basicVideo"></iframe>

## Resources

- Detailed documentation for this app-template is available at :

   <a
     href="http://docs.datatorrent.com/app-templates/0.10.0/"  class="docs" id="docs" ga-track="docs"
     target="_blank">http://docs.datatorrent.com/app-templates/</a>
- Source code for this app-template is available at :

    <a
     href="https://github.com/DataTorrent/moodI/tree/master/app-templates/kafka-to-hdfs-filter-transform"  class="github" id="github" ga-track="github" target="_blank">https://github.com/DataTorrent/moodI/tree/master/app-templates/kafka-to-hdfs-filter-transform</a>

- Please send feedback or feature requests to :
    <a href="mailto:feedback@datatorrent.com"  class="feedback" id="feedback" ga-track="feedback">feedback@datatorrent.com</a>

- Join our user discussion group at :
    <a href="mailto:dt-users@googlegroups.com"  class="maillist" id="maillist" ga-track="maillist">dt-users@googlegroups.com</a>
