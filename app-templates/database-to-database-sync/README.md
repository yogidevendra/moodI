## Description

This application demonstrates continuous archival of big data from database tables.
It first does a bulk upload from the configured table parallely and continuously polls at configured poll interval for new records and writes them in destination table.

- The application scales linearly with the number of poller partitions.
- The application is fault tolerant and can withstand node and cluster outages without data loss.
- The application is also highly performant and can process as fast as the network allows.
- It is extremely easy to add custom logic to get your business value without worrying about database connectivity and operational details of database poller and database writer.
- The only configuration user needs to provide is source database connection details, table.
- This enterprise grade application template will dramatically reduce your time to market and cost of operations.

## Implementation details

- Logical flow diagram

   ![Logical Plan](https://www.datatorrent.com/wp-content/uploads/2016/12/db_to_db_sync_DAG.png)
- It uses following operators
  - JDBC POJO Poll Input operator
  - JDBC POJO Insert output operator
- Supported data source
  - PostgreSQL version: 9.4
  - Tested with PostgreSQL client library: org.postgresql:postgresql:9.4.1208.jre7
- Supported sinks
  - PostgreSQL version: 9.4
  - Tested with PostgreSQL client library: org.postgresql:postgresql:9.4.1208.jre7

## Supported visualizations

  | Description  | Widget   |
  |---|---|
  | Events read per minute from Source Database  | Line chart|
  | Events written per minute to Sink Database | Line chart |

## Resources

  - Detailed documentation for this app-template is available at :

     <a
       href="http://docs.datatorrent.com/app-templates/0.10.0/database-to-database-sync/"  class="docs" id="docs" ga-track="docs"
       target="_blank">http://docs.datatorrent.com/app-templates/0.10.0/database-to-database-sync/</a>
  - Source code for this app-template is available at :

      <a
       href="https://github.com/DataTorrent/moodI/tree/master/app-templates/database-to-database-sync"  class="github" id="github" ga-track="github" target="_blank">https://github.com/DataTorrent/moodI/tree/master/app-templates/database-to-database-sync</a>

  - Please send feedback or feature requests to :
      <a href="mailto:feedback@datatorrent.com"  class="feedback" id="feedback" ga-track="feedback">feedback@datatorrent.com</a>

  - Join our user discussion group at :
      <a href="mailto:dt-users@googlegroups.com"  class="maillist" id="maillist" ga-track="maillist">dt-users@googlegroups.com</a>
