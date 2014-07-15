# jaws-spark-sql-rest

Restful service for running Spark SQL/Shark queries on top of Spark, with Mesos and Tachyon support (codenamed jaws)
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
    curl -d "select * from table" 'http://devbox.local:8181/jaws/run?limited=true&numberOfResults=99' -X POST

Parameters:
   
  * limited [required]:  if set on true, the query will be limited to a fixed number of results. The number of results will be the one specified in the "numberOfResults" parameter, and they will be collected (in memory) and persisted in the configured backend database (cassandra or hdfs, no latency difference upon retrieval of small datasets). Otherwise, if not specified, the results number will be retrieved from the configuration file (nr.of.results field, default is 100).
  However, for large datasets that exceed the default number of results (100, configurable), results will not be persisted in memory and the configured database anymore, they will only be stored as an RDD on HDFS, and used for paginated retrieval (offset and limit parameters in the results api).
  If the limited parameter is set on false, then the query will return all the results and this time they will be stored in an RDD on HDFS, this is an indicator that a large dataset is about to be queried.
  * numberOfResults [not required]: The parameter is considered only if the "limited" parameter is set on true

Results:

The api returns an uuid representing the query that was submitted. The query is executed asynchronously and the expectation is that clients poll for logs and completion status.

Exemple:
 1404998257416357bb29d-6801-41ca-82e4-7a265816b50c
 

### Logs api:
    curl 'http://devbox.local:8181/jaws/logs?queryID=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7&startTimestamp=0&limit=10' -X GET

Parameters:

  * queryID [required] : is the uuid returned by the run api.
  * startTimestamp [not required] : start timestamp from where the logs will be returned
  * limit [required] : number of log entries to be returned

Results:

The api returns a JSON with a list of log entries and the status of the submitted query.

Exemple:

{

    "logs": [
        {
            "log": "There are 2 commands that need to be executed",
            "queryID": "hql",
            "timestamp": 1404998257430
        },
        {
            "log": "Command progress : There were executed 1 commands out of 2",
            "queryID": "hql",
            "timestamp": 1404998257501
        },
        {
            "log": "The job 16 has started. Executing command --1404998257416357bb29d-6801-41ca-82e4-7a265816b50c\nselect UPkmbVaZXr.* from ( select * from user_predictions limit 3) UPkmbVaZXr limit 100",
            "queryID": "16",
            "timestamp": 1404998258154
        }
    ],
    "status": "DONE"
}

### Results api:
    curl 'http://devbox.local:8181/jaws/results?queryID=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7&offset=0&limit=10' -X GET

Parameters:

  * queryID [required] : uuid returned by the run api. This uuid represents the submitted query
  * offset [required] : starting result entry that needs to be returned
  * limit [required] : number of results to be returned (page size)

Results:

The api returns a JSON containing the results schema and a list of result entries. The results are retrieved in pages, with the size specified by the limit parameter, starting with offset.

Example:
{

    "schema": [
        "userid",
        "moviename"
    ],
    "results": [
        [
            "269",
            "Blade Runner (1982)"
        ],
        [
            "234",
            "Lone Star (1996)"
        ],
        [
            "92",
            "Grand Day Out, A (1992)"
        ]
    ]
}


### Queries api:
    curl 'http://devbox.local:8181/jaws/queries?startQueryID=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7&limit=50' -X GET

Parameters:

  * startQueryID [not required] : uuid of the first executed query in the list to be returned
  * limit [required] : number of queries to be returned

Results:

The api returns a JSON containing a list of queries and associated meta information, in chronological order (most recent at the top)

Example:
{

    "queries": [
        {
            "state": "DONE",
            "queryID": "1404998257416357bb29d-6801-41ca-82e4-7a265816b50c",
            "query": "USE test;\n\nselect * from user_predictions limit 3"
        },
        {
            "state": "DONE",
            "queryID": "14049979591823fde2ad2-3dcd-4cc9-92ec-4a65745f0d12",
            "query": "drop table if exists ii_tachyon;\ncreate table ii_tachyon as select * from varsta;\nselect * from ii_tachyon;"
        },
        {
            "state": "FAILED",
            "queryID": "1404977509762f832602b-2ad8-423f-ab45-97ebcc354167",
            "query": "USE test;\n\nrefresh;\n\nselect userid from ema_tachyon;"
        }
    ]
}

### queryInfo api:
    curl 'http://devbox.local:8181/jaws/queryInfo?queryID=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7' -X GET

Parameters:

  * queryID [required] : uuid returned by the run api.

Results:

The api returns the status and the query string (could be one statement or an entire HiveQL script)

Example:

        {
            "state": "DONE",
            "queryID": "1404998257416357bb29d-6801-41ca-82e4-7a265816b50c",
            "query": "USE test;\n\nselect * from user_predictions limit 3"
        }

### Databases api: 
    curl 'http://devbox.local:8181/jaws/databases' -X GET

Results:

The api returns a JSON containing a list of existing databases.

{

    "schema": [
        "database_name"
    ],
    "results": [
        [
            "default"
        ],
        [
            "demo"
        ],
        [
            "integrationtest"
        ]
  ]
}

Example:

### Cancel api:
    curl 'http://devbox.local:8181/jaws/cancel?queryID=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7' -X POST

Parameters:

  * queryID [required] : uuid returned by the run api.

This api cancels a running query. Unless Jaws runs in fine-grained mode under Mesos, the underlying Spark job is also cancelled. Spark job cancellation in Mesos fine-grained mode is not implemented in Spark core yet! In this mode, if the query is still in the queue, it won't be executed, but we cannot stop it once it started.
 

### Tables api:

    curl 'http://devbox.local:8181/jaws/tables' -X GET
    curl 'http://devbox.local:8181/jaws/tables?database=default' -X GET

Parameters:

  * database [not required] : is the database for which you want to retrieve all the tables

Results:

If the database parameter is set, the api returns a JSON containing all the tables from the specified database with their columns, otherwise, it will return all the databases with all their tables and their columns.

Example:

{

    "test": {
        "user_predictions": {
            "schema": [
                "col_name",
                "data_type",
                "comment"
            ],
            "results": [
                [
                    "userid",
                    "int",
                    "None"
                ],
                [
                    "moviename",
                    "string",
                    "None"
                ]
            ]
        },
        "testsimple": {
            "schema": [
                "col_name",
                "data_type",
                "comment"
            ],
            "results": [
                [
                    "ip",
                    "string",
                    "None"
                ],
                [
                    "time",
                    "string",
                    "None"
                ],
                [
                    "state",
                    "string",
                    "None"
                ],
                [
                    "reason",
                    "string",
                    "None"
                ]
            ]
        },
    }
}
