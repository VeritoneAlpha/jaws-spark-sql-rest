# jaws-spark-sql-rest

Restful service for running and Spark SQL/Shark queries on top of Spark, with Mesos and Tachyon support (codenamed jaws)
Currently, only Spark 0.9.x and Shark are supported as backend framework for Jaws, will add support for Spark 1.0 and Spark SQL very soon.


## Features

Highly scalable and resilient restful (http) interface on top of a managed Spark SQL/Shark session that can concurrently and asynchronously submit HiveQL queries, return persisted results (automatically limited in size or paged), execution logs and query history (Cassandra or hdfs persisted).
Jaws supports query cancellation even when there are multiple load balanced instances for higher availability and scalability.
Jaws exposes configuration options for fine-tuning Spark & Shark performance and running against a stand-alone Spark deployment, with or without Tachyon as in-memory distributed file system on top of HDFS, and with or without Mesos as resource manager
We are using this service for building a warehouse explorer GUI component, that provides data analysts and scientists with a view into the warehouse through a metadata explorer, provides a query editor with intelligent features like auto-complete, a results viewer, logs viewer and historical queries for asynchronously retrieving persisted results, logs and query information for both running and historical queries.

For Spark 0.9.x Jaws uses Shark against tables created through Hive, Shark or Tachyon.
For Spark 1.0 it uses SparkSQL against Hive tables and Parquet files stored in hdfs and it is not backward compatible with Shark tables created in 0.9.x.



## Building Jaws

You need to have Maven installed.
To build Jaws, follow the below steps:

	cd http-spark-sql-server
	mvn clean install -DskipTests
	cd xpatterns-jaws/target
	tar -zxvf xpatterns-jaws.tar.gz

## Configure Jaws

In order configure Jaws the following files inside the "http-spark-sql-server/xpatterns-jaws/target/xpatterns-jaws/conf/"  need to be edited.

    * application.conf: Contains all the application configurations.
    * hive-site.xml : Used for setting values for the Hive configuration
    * jaws-env.sh : Used for setting the environment variables
    * log4j.properties : Used for configuring the logger
    * sharkSettings.txt : Used to write all the shark commands that you need to be run before launching jaws. (One command per line. Example: set shark.column.compress=true)


## Run jaws

After editing all the configuration files Jaws can be run in the following manner:
    
    http-spark-sql-server/xpatterns-jaws/target/xpatterns-jaws/bin/start-jaws.sh
    

## Queries examples

Below are some queries with example purpose:
### Run api:
    curl -d "select * from table" 'http://devbox.local:8181/jaws/run?limited=true&resultsnumber=99' -X POST

### Logs api:
    curl 'http://devbox.local:8181/jaws/logs?uuid=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7&starttimestamp=0&limit=10' -X GET 

### Results api:
    curl 'http://devbox.local:8181/jaws/results?uuid=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7&offset=0&limit=10' -X GET 

### Jobs api: 
    curl 'http://devbox.local:8181/jaws/jobs?startUuid=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7&limit=50' -X GET 

### Description api:
    curl 'http://devbox.local:8181/jaws/description?uuid=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7' -X GET 

### Databases api: 
    curl 'http://devbox.local:8181/jaws/databases' -X GET

### Cancel api:
    curl 'http://devbox.local:8181/jaws/cancel?uuid=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7' -X POST  

### Tables api:

    curl 'http://devbox.local:8181/jaws/tables' -X GET
    curl 'http://devbox.local:8181/jaws/tables?database=default' -X GET