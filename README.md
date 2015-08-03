# jaws-spark-sql-rest

Restful service for running Spark SQL/Shark queries on top of Spark, with Mesos and Tachyon support (codenamed jaws)
Curently Jaws supports Spark 0.9.1 with Shark and Spark 1.0.1, 1.0.2, 1.1.0 and 1.0.2 with SparkSQL as backend framework.


## Features

Highly scalable and resilient restful (http) interface on top of a managed Spark SQL/Shark session that can concurrently and asynchronously submit HiveQL queries, return persisted results (automatically limited in size or paged), execution logs and query history (Cassandra or hdfs persisted).
Jaws supports query cancellation even when there are multiple load balanced instances for higher availability and scalability.
Jaws exposes configuration options for fine-tuning Spark & Shark performance and running against a stand-alone Spark deployment, with or without Tachyon as in-memory distributed file system on top of HDFS, and with or without Mesos as resource manager.
We are using this service for building a warehouse explorer GUI component, that provides data analysts and scientists with a view into the warehouse through a metadata explorer, provides a query editor with intelligent features like auto-complete, a results viewer, logs viewer and historical queries for asynchronously retrieving persisted results, logs and query information for both running and historical queries.

For Spark 0.9.x Jaws uses Shark against tables created through Hive, Shark or Tachyon.
For Spark 1.0 and 1.1 it uses SparkSQL against Hive tables and Parquet files stored in hdfs or tachyon and it is not backward compatible with Shark tables created in 0.9.x.

Jaws offers the possibility to plug a UI. There is a open sourced UI available at: 

    https://github.com/iLiviu/JawsUI

Also, for the parquet table creation feature to be available in UI, the service that lists the files on hdfs and tachyon has to be deployed. This service is available at :

    https://github.com/emaorhian/hdfs-tachyon-file-browser
    
     

## Building Jaws

You need to have Maven installed.
To build Jaws, follow the below steps:

    cd jaws-spark-sql-rest
    mvn clean install -DskipTests
    cd jaws-spark-sql-rest/target
    tar -zxvf jaws-spark-sql-rest.tar.gz

## Configure Jaws

In order configure Jaws the following files inside the "jaws-spark-sql-rest/jaws-spark-sql-rest/target/jaws-spark-sql-rest/conf/"  need to be edited.

    * application.conf: Contains all the application configurations.
    * hive-site.xml : Used for setting values for the Hive configuration
    * jaws-env.sh : Used for setting the environment variables
    * log4j.properties : Used for configuring the logger
    * sharkSettings.txt : Used to write all the shark commands that you need to be run before launching jaws. (One command per line. Example: set shark.column.compress=true)

For Spark 1.* don't forget to set the following property on false in hive-site.xml:
 
       <property>
             <name>hive.support.concurrency</name>
             <value>false</value>
       </property>

If you are running on top of mesos, don't forget to add in the jaws-env.sh file the path to the native mesos library.

If your tables are snappy compressed, don't forget to add in the jaws-env.sh the path to the hadoop native libs.

### Plugin a UI for Jaws

To plugin your UI in jaws you just have to copy your app in the jaws-spark-sql-rest/src/main/webapp folder.

The Jaws UI will be available at the following url:
    
    http://devbox.local:9080/jaws/ui/


## Run jaws

After editing all the configuration files Jaws can be run in the following manner:

    1. go where you decompressed the tar.gz file:
       cd jaws-spark-sql-rest
    2. run Jaws:
       nohup bin/start-jaws.sh &

### Verify that Jaws spark sql is up

    curl 'devbox.local:9080/jaws/index' -X GET
    
Results:
You will receive the following confirmation message that Jaws hive sql is up and running:

"Jaws is up and running!"

## Apis usage examples

Below is described Jaws functionality throgh api calls examples and sample results.

### Run queries apis
Below is described how a user could run queries, retrieve logs, results, ask for queries information, even delete or cancel a query

#### Run query
    curl -d "select * from table" 'http://devbox.local:9080/jaws/run?limited=true&numberOfResults=99'-X POST

Parameters:

  * limited [required]:  if set on true, the query will be limited to a fixed number of results. The number of results will be the one specified in the "numberOfResults" parameter, and they will be collected (in memory) and persisted in the configured backend database (cassandra or hdfs, no latency difference upon retrieval of small datasets). Otherwise, if not specified, the results number will be retrieved from the configuration file (nr.of.results field, default is 100).
  However, for large datasets that exceed the default number of results (100, configurable), results will not be persisted in memory and the configured database anymore, they will only be stored as an RDD on HDFS, and used for paginated retrieval (offset and limit parameters in the results api).
  If the limited parameter is set on false, then the query will return all the results and this time they will be stored in an RDD on HDFS, this is an indicator that a large dataset is about to be queried.
  * numberOfResults [not required]: The parameter is considered only if the "limited" parameter is set on true
  * destination [not required]: Default value hdfs, possible values hdfs/tachyon. This field is used for unlimited results queries, to specify where the result RDD will be persisted.

Results:

The api returns an uuid representing the query that was submitted. The query is executed asynchronously and the expectation is that clients poll for logs and completion status.

Example:

 1404998257416357bb29d-6801-41ca-82e4-7a265816b50c

#### Logs api:

This api is used for retrieving paginated logs for a specific query

    curl 'http://devbox.local:9080/jaws/logs?queryID=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7&startTimestamp=0&limit=3' -X GET

Parameters:

  * queryID [required] : is the uuid returned by the run api.
  * startTimestamp [not required] : start timestamp from where the logs will be returned
  * limit [required] : number of log entries to be returned

Results:

The api returns a JSON with a list of log entries and the status of the submitted query.

Exemple:

    {
      "logs": [{
        "log": "Launching task for 1432023553359888c5575-68d8-414a-826d-4d0ae903694b",
        "queryID": "hql",
        "timestamp": 1432023553360
      }, {
        "log": "There are 1 commands that need to be executed",
        "queryID": "hql",
        "timestamp": 1432023553367
      }, {
        "log": "The job 4 has started. Executing command.",
        "queryID": "4",
        "timestamp": 1432023553493
      }],
      "status": "DONE"
    }
    
#### Results api:

This api is used to return results for a certain query. The api offers 3 results formats: 
- avro json format
- avro binary format (bytes base64 encoded)
- default format
 
Note : Jaws supports results retrieval for nested types. 


    curl 'http://devbox.local:9080/jaws/results?queryID=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7&offset=0&limit=10&format=default' -X GET

Parameters:

  * queryID [required] : uuid returned by the run api. This uuid represents the submitted query
  * offset [required] : starting result entry that needs to be returned
  * limit [required] : number of results to be returned (page size)
  * format [not required] : the format in which the results should be returned. Possible values :  "avrobinary"/"avrojson"/"default"

Results:
*  Custom result format
 
    {
        "schema": [
            {
                "name": "id",
                "dataType": "StringType",
                "comment": "",
                "members": []
            }
        ],
        "result": [
            [
                "353a3ba1fd1b478a8b41544c5442b53a2a29c8ce425c537dd86ec9d34960f0c2"
            ]
        ]
    }

*  Avro binary format
    
        {
        "schema": {
            "fields": [
                {
                    "name": "id",
                    "schema": {
                        "types": [
                            {
                                "type": "STRING",
                                "hashCode": -2147483648,
                                "props": {},
                                "reserved": [
                                    "values",
                                    "symbols",
                                    "items",
                                    "name",
                                    "doc",
                                    "type",
                                    "aliases",
                                    "size",
                                    "namespace",
                                    "fields"
                                ]
                            },
                            {
                                "type": "NULL",
                                "hashCode": -2147483648,
                                "props": {},
                                "reserved": [
                                    "values",
                                    "symbols",
                                    "items",
                                    "name",
                                    "doc",
                                    "type",
                                    "aliases",
                                    "size",
                                    "namespace",
                                    "fields"
                                ]
                            }
                        ],
                        "indexByName": {
                            "string": 0,
                            "null": 1
                        },
                        "type": "UNION",
                        "hashCode": -2147483648,
                        "props": {},
                        "reserved": [
                            "values",
                            "symbols",
                            "items",
                            "name",
                            "doc",
                            "type",
                            "aliases",
                            "size",
                            "namespace",
                            "fields"
                        ]
                    },
                    "order": "ASCENDING",
                    "props": {},
                    "reserved": [
                        "default",
                        "order",
                        "name",
                        "doc",
                        "type",
                        "aliases"
                    ]
                }
            ],
            "fieldMap": {
                "id": {
                    "name": "id",
                    "schema": {
                        "types": [
                            {
                                "type": "STRING",
                                "hashCode": -2147483648,
                                "props": {},
                                "reserved": [
                                    "values",
                                    "symbols",
                                    "items",
                                    "name",
                                    "doc",
                                    "type",
                                    "aliases",
                                    "size",
                                    "namespace",
                                    "fields"
                                ]
                            },
                            {
                                "type": "NULL",
                                "hashCode": -2147483648,
                                "props": {},
                                "reserved": [
                                    "values",
                                    "symbols",
                                    "items",
                                    "name",
                                    "doc",
                                    "type",
                                    "aliases",
                                    "size",
                                    "namespace",
                                    "fields"
                                ]
                            }
                        ],
                        "indexByName": {
                            "string": 0,
                            "null": 1
                        },
                        "type": "UNION",
                        "hashCode": -2147483648,
                        "props": {},
                        "reserved": [
                            "values",
                            "symbols",
                            "items",
                            "name",
                            "doc",
                            "type",
                            "aliases",
                            "size",
                            "namespace",
                            "fields"
                        ]
                    },
                    "order": "ASCENDING",
                    "props": {},
                    "reserved": [
                        "default",
                        "order",
                        "name",
                        "doc",
                        "type",
                        "aliases"
                    ]
                }
            },
            "isError": false,
            "name": {
                "name": "RECORD",
                "full": "RECORD"
            },
            "type": "RECORD",
            "hashCode": 1530317918,
            "props": {},
            "reserved": [
                "values",
                "symbols",
                "items",
                "name",
                "doc",
                "type",
                "aliases",
                "size",
                "namespace",
                "fields"
            ]
        },
        "result": "T2JqAQIWYXZyby5zY2hlbWGmAXsidHlwZSI6InJlY29yZCIsIm5hbWUiOiJSRUNPUkQiLCJmaWVsZHMiOlt7Im5hbWUiOiJpZCIsInR5cGUiOlsic3RyaW5nIiwibnVsbCJdfV19AHFrB9h2Q6CTwaMiKMEqR2wChgEAgAEzNTNhM2JhMWZkMWI0NzhhOGI0MTU0NGM1NDQyYjUzYTJhMjljOGNlNDI1YzUzN2RkODZlYzlkMzQ5NjBmMGMycWsH2HZDoJPBoyIowSpHbA=="
    }

*  Avro json format
    
        [
        {
            "schema": {
                "fields": [
                    {
                        "name": "id",
                        "schema": {
                            "types": [
                                {
                                    "type": "STRING",
                                    "hashCode": -2147483648,
                                    "props": {},
                                    "reserved": [
                                        "values",
                                        "symbols",
                                        "items",
                                        "name",
                                        "doc",
                                        "type",
                                        "aliases",
                                        "size",
                                        "namespace",
                                        "fields"
                                    ]
                                },
                                {
                                    "type": "NULL",
                                    "hashCode": -2147483648,
                                    "props": {},
                                    "reserved": [
                                        "values",
                                        "symbols",
                                        "items",
                                        "name",
                                        "doc",
                                        "type",
                                        "aliases",
                                        "size",
                                        "namespace",
                                        "fields"
                                    ]
                                }
                            ],
                            "indexByName": {
                                "string": 0,
                                "null": 1
                            },
                            "type": "UNION",
                            "hashCode": -2147483648,
                            "props": {},
                            "reserved": [
                                "values",
                                "symbols",
                                "items",
                                "name",
                                "doc",
                                "type",
                                "aliases",
                                "size",
                                "namespace",
                                "fields"
                            ]
                        },
                        "order": "ASCENDING",
                        "props": {},
                        "reserved": [
                            "default",
                            "order",
                            "name",
                            "doc",
                            "type",
                            "aliases"
                        ]
                    }
                ],
                "fieldMap": {
                    "id": {
                        "name": "id",
                        "schema": {
                            "types": [
                                {
                                    "type": "STRING",
                                    "hashCode": -2147483648,
                                    "props": {},
                                    "reserved": [
                                        "values",
                                        "symbols",
                                        "items",
                                        "name",
                                        "doc",
                                        "type",
                                        "aliases",
                                        "size",
                                        "namespace",
                                        "fields"
                                    ]
                                },
                                {
                                    "type": "NULL",
                                    "hashCode": -2147483648,
                                    "props": {},
                                    "reserved": [
                                        "values",
                                        "symbols",
                                        "items",
                                        "name",
                                        "doc",
                                        "type",
                                        "aliases",
                                        "size",
                                        "namespace",
                                        "fields"
                                    ]
                                }
                            ],
                            "indexByName": {
                                "string": 0,
                                "null": 1
                            },
                            "type": "UNION",
                            "hashCode": -2147483648,
                            "props": {},
                            "reserved": [
                                "values",
                                "symbols",
                                "items",
                                "name",
                                "doc",
                                "type",
                                "aliases",
                                "size",
                                "namespace",
                                "fields"
                            ]
                        },
                        "order": "ASCENDING",
                        "props": {},
                        "reserved": [
                            "default",
                            "order",
                            "name",
                            "doc",
                            "type",
                            "aliases"
                        ]
                    }
                },
                "isError": false,
                "name": {
                    "name": "RECORD",
                    "full": "RECORD"
                },
                "type": "RECORD",
                "hashCode": -1456956472,
                "props": {},
                "reserved": [
                    "values",
                    "symbols",
                    "items",
                    "name",
                    "doc",
                    "type",
                    "aliases",
                    "size",
                    "namespace",
                    "fields"
                ]
            },
            "values": [
                "353a3ba1fd1b478a8b41544c5442b53a2a29c8ce425c537dd86ec9d34960f0c2"
            ]
        }
    ]
    
#### Queries api:

This api returns information about the executed queries. You can request information about queries either in a paginated manner, either specifying the list of queries you want to obtain. 

1. retrieve queries info paginated: 

        curl 'http://devbox.local:9080/jaws/queries?startQueryID=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7&limit=50' -X GET
     
2. retrieve info for a certain list of queries 
    
        curl 'http://devbox.local:9080/jaws/queries?queryID=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7&queryID=142962545249766b92481-f437-4ee2-9284-cf192fcc2769' -X GET

Parameters:

  * startQueryID [not required] : (default first query) uuid of the first executed query in the list to be returned
  * limit [not required] :(default 100) number of queries to be returned
  * queryID [not required] : the query uuid to retrieve information about

Results:

The api returns a JSON containing a list of queries and associated meta information. If there isn't any specific query id requested, then the api will return the queries paginated, in chronological order (most recent at the top). 

Example:
    
     {
      "queries": [{
        "state": "DONE",
        "queryID": "1432051694672f716608c-49a1-4dfe-ab77-8f98810b317f",
        "query": "select * from test limit 1",
        "metaInfo": {
          "executionTime": 100,
          "timestamp": 1438604292241,                
          "nrOfResults": 1,
          "maxNrOfResults": 3,
          "resultsDestination": 0,
          "isLimited": true
        }
      }, {
        "state": "DONE",
        "queryID": "143205136408871e22b82-e1b7-4b95-ac0f-1c798154628e",
        "query": "select * from x limit 1",
        "metaInfo": {
          "executionTime": 100,
          "timestamp": 1438604592241,
          "nrOfResults": 1,
          "maxNrOfResults": 3,
          "resultsDestination": 0,
          "isLimited": true
        }
      }]
    }



#### Delete query api:

This is an API that deletes from the database all the information about a query:
    - query state
    - query details
    - query logs
    - query meta info
    - query results

    curl 'http://devbox.local:9080/jaws/queries/{queryID}' -X DELETE
    

Path Segment :

  * queryID [required] : represents the query that needs to be deleted

If the query is in progress or it is not found, an explanatory error message will be thrown.   

 Results:

"Query 140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7 was deleted"

#### Cancel api:

This api cancels a running query. Unless Jaws runs in fine-grained mode under Mesos, the underlying Spark job is also cancelled. Spark job cancellation in Mesos fine-grained mode is not implemented in Spark core yet! In this mode, if the query is still in the queue, it won't be executed, but we cannot stop it once it started.

    curl 'http://devbox.local:9080/jaws/cancel?queryID=140413187977964cf5f85-0dd3-4484-84a3-7703b098c2e7' -X POST

Parameters:

  * queryID [required] : uuid returned by the run api.


###Parquet management apis
The following apis are used to register a parquet table, run queries against it, list the registered parquet tables and delete them.

#### Run parquet api:

This api is deprecated. You should first register the parquet table(see register api detailed below), and then query it with the RUN api explained above. However, the run parquet api is still functional.

    curl -d "select * from testTable" 'http://devbox.local:9080/jaws/parquet/run?tablePath=/user/jaws/parquetFolder&pathType=tachyon&table=testTable&limited=true&numberOfResults=99&overwrite=false' -X POST

Parameters:

  * tablePath [required] : the path to the parquet folder which you want to query
  * pathType [not required, default hdfs]: the type of parquet folder path. It can be **tachyon** or **hdfs**. The values for namenodes are filled from the configuration.  
  * table : the table name you want to give to your parquet folder
  * limited [required]:  if set on true, the query will be limited to a fixed number of results. The number of results will be the one specified in the "numberOfResults" parameter, and they will be collected (in memory) and persisted in the configured backend database (cassandra or hdfs, no latency difference upon retrieval of small datasets). Otherwise, if not specified, the results number will be retrieved from the configuration file (nr.of.results field, default is 100).
  However, for large datasets that exceed the default number of results (100, configurable), results will not be persisted in memory and the configured database anymore, they will only be stored as an RDD on HDFS, and used for paginated retrieval (offset and limit parameters in the results api).
  If the limited parameter is set on false, then the query will return all the results and this time they will be stored in an RDD on HDFS, this is an indicator that a large dataset is about to be queried.
  * numberOfResults [not required]: The parameter is considered only if the "limited" parameter is set on true
  * overwrite [not required, default false] : if set on false and the table already exists, an error message will be returned to the client saying that the table already exists. If set on true, the table will be overwritten


Results:

The api returns an uuid representing the query that was submitted. The query is executed asynchronously and the expectation is that clients poll for logs and completion status.

Example:
 1404998257416357bb29d-6801-41ca-82e4-7a265816b50c

####Register parquet table api:
    curl 'http://devbox.local:9080/jaws/parquet/tables?path=/user/jaws/parquetFolder&pathType=tachyon&name=testTable&overwrite=false' -X POST

Parameters:

  * path [required] : the path to the parquet folder you want to register as table
  * pathType [not required, default hdfs]: the type of parquet folder path. It can be **tachyon** or **hdfs**. The values for namenodes are filled from the configuration.
  * name [reqired] : the table name you want to give to your parquet folder
  * overwrite [not required, default false] : if set on false and the table already exists, an error message will be returned to the client saying that the table already exists. If set on true, the table will be overwritten


Results:
    
The api returns message confirming the table registration or an error message in case of failure

Exemple:
 Table testTable was registered

### List parquet tables api:

Note: Jaws supports describing tables with nested schema types

    curl 'http://devbox.local:9080/jaws/parquet/tables' -X GET
    curl 'http://devbox.local:9080/jaws/parquet/tables?describe=true' -X GET
    curl 'http://devbox.local:9080/jaws/parquet/tables?table=table1&table=table2' -X GET

Parameters:

  * describe [not required] : the default value is false. Flag that specifies if describe table should be performed
  * table [not required] : lists the tables that will be described.

Results:

If no parameter is set, the api returns a JSON containing all the registered parquet tables without their schema.
If the describe parameter is set on true, the api will return all the registered parquet tables with their schema.
If a table list is provided, then those will be the tables that will be described.

Example:

1. list parquet tables without schema:

        [{
              "database": "None",
              "tables": [{
                "name": "complex",
                "columns": [],
                "extraInfo": []
              }, {
                "name": "complex2",
                "columns": [],
                "extraInfo": []
              }, {
                "name": "complexTest",
                "columns": [],
                "extraInfo": []
              }, {
                "name": "testTable",
                "columns": [],
                "extraInfo": []
              }]
     
2. List parquet tables with their schema

        [{
          "database": "None",
          "tables": [{
            "name": "complex",
            "columns": [{
              "name": "s",
              "dataType": "StringType",
              "comment": "",
              "members": []
            }, {
              "name": "obj",
              "dataType": "StructType",
              "comment": "",
              "members": [{
                "name": "myString",
                "dataType": "StringType",
                "comment": "",
                "members": []
              }, {
                "name": "myInteger",
                "dataType": "IntegerType",
                "comment": "",
                "members": []
              }]
            }],
            "extraInfo": []
          }]

#### Unregister parquet table api:
    curl 'http://devbox.local:9080/jaws/parquet/tables/{name}' -X DELETE

Parameters:

  * name [required] : the table name you want to unregister
  

Results:

The api returns message confirming that table was unregistered successfully, or an error message in case of failure

Example:
 Table testTable was unregistered


###Browse warehouse apis
The following apis are used to browse the warehouse: show databases, tables, describing tables.

#### Databases api:
    curl 'http://devbox.local:9080/jaws/hive/databases' -X GET

Results:

The api returns a JSON containing a list of existing databases.

Example:

    {
        "databases": [
            "default",
            "demo",
            "sample"     
        ]
    }

#### Schema api:

    curl 'http://devbox.local:9080/jaws/schema?path=databaseName.tableName&sourceType=hive' -X GET
    curl 'http://devbox.local:9080/jaws/schema?path=/user/test/location&sourceType=parquet&storageType=tachyon' -X GET
    curl 'http://devbox.local:9080/jaws/schema?path=/user/test/location&sourceType=parquet' -X GET

Parameters:

  * path [required] : represents the location of the data
  * sourceType [required]: represents the source you want to read the data from; supported values are HIVE and PARQUET
  * storageType [not required]: represents the persistence layer the PARQUET data is stored in; supported values are TACHYON and HDFS (default)


Results:

The api returns a JSON containing the schema (in AVRO format) of the specified data source.


Example:

    {
    "type": "record",
    "name": "RECORD",
    "fields": [
        {
            "name": "id",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "owner",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "subject",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "from",
            "type": [
                {
                    "type": "array",
                    "items": [
                        "string",
                        "null"
                    ]
                },
                "null"
            ]
        },
        {
            "name": "replyTo",
            "type": [
                {
                    "type": "array",
                    "items": [
                        "string",
                        "null"
                    ]
                },
                "null"
            ]
        },
        {
            "name": "sentDate",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "toRecipients",
            "type": [
                {
                    "type": "array",
                    "items": [
                        "string",
                        "null"
                    ]
                },
                "null"
            ]
        },
        {
            "name": "ccRecipients",
            "type": [
                {
                    "type": "array",
                    "items": [
                        "string",
                        "null"
                    ]
                },
                "null"
            ]
        },
        {
            "name": "bccRecipients",
            "type": [
                {
                    "type": "array",
                    "items": [
                        "string",
                        "null"
                    ]
                },
                "null"
            ]
        },
        {
            "name": "receivedDate",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "content",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "messageId",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "contentId",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "isReplica",
            "type": "boolean"
        },
        {
            "name": "replicas",
            "type": [
                {
                    "type": "array",
                    "items": [
                        "string",
                        "null"
                    ]
                },
                "null"
            ]
        }
    ]}


### Tables api:

    curl 'http://devbox.local:9080/jaws/hive/tables' -X GET
    curl 'http://devbox.local:9080/jaws/hive/tables?database=default' -X GET
    curl 'http://devbox.local:9080/jaws/hive/tables?database=default&describe=true' -X GET
    curl 'http://devbox.local:9080/jaws/hive/tables?database=default&table=table1&table=table2' -X GET

Parameters:

  * database [not required] : is the database for which you want to retrieve all the tables.
  * describe [not required] : the default value is true. Flag that specifies if describe table should be performed
  * tables [not required] : lists the tables that will be described.

Results:

If the database parameter is set, the api returns a JSON containing all the tables from the specified database, otherwise, it will return all the databases with all their tables.
If the describe parameter is set on true, also the tables columns are returned.
If a table list is provided, then those will be the tables that will be described. (The database is needed)

Example:

    [{
      "database": "default",
      "tables": [{
        "name": "x",
        "columns": [{
          "name": "id",
          "dataType": "string",
          "comment": "None",
          "members": []
        }],
        "extraInfo": []
      }, {
        "name": "comm",
        "columns": [{
          "name": "nume",
          "dataType": "string",
          "comment": "comentariu",
          "members": []
        }, {
          "name": "varsta",
          "dataType": "int",
          "comment": "comentariu2",
          "members": []
        }],
        "extraInfo": []
      }]

#### Tables extended api:

    curl 'http://devbox.local:9080/jaws/hive/tables/extended?database=default&table=test' -X GET
    curl 'http://devbox.local:9080/jaws/hive/tables/extended?database=default' -X GET

Parameters:

  * database [required] : is the database for which you want to retrieve extended information about tables
  * table [not required]: is the table for which you want to retrieve extended information


Results:

If the table parameter is set, the api returns a JSON containing the extended information about it. Otherwise, the api will return the extended information about all the tables inside the specified database.
The extra information the extended table api offers, will be in the "extraInfo" field

Example:

    [
        {
            "database": "default",
            "tables": [
                {
                    "name": "x",
                    "columns": [
                        {
                            "name": "id",
                            "dataType": "string",
                            "comment": "None",
                            "members": []
                        }
                    ],
                    "extraInfo": [
                        [
                            "Detailed Table Information",
                            "Table(tableName:x, dbName:default, owner:ubuntu, createTime:1427875914, lastAccessTime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:id, type:string, comment:null)], location:hdfs://main:8020/user/hive/warehouse/x, inputFormat:org.apache.hadoop.mapred.TextInputFormat, outputFormat:org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat, compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, parameters:{serialization.format=1}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[], parameters:{numFiles=1, transient_lastDdlTime=1427875914, COLUMN_STATS_ACCURATE=true, totalSize=195, numRows=3, rawDataSize=192}, viewOriginalText:null, viewExpandedText:null, tableType:MANAGED_TABLE)"
                        ]
                    ]
                }
            ]
        }
    ]


### Tables formatted api:

    curl 'http://devbox.local:9080/jaws/hive/tables/formatted?database=default&table=test' -X GET
    curl 'http://devbox.local:9080/jaws/hive/tables/formatted?database=default' -X GET

Parameters:

  * database [required] : is the database for which you want to retrieve formatted information about tables
  * table [not required]: is the table for which you want to retrieve formatted information


Results:

If the table parameter is set, the api returns a JSON containing the formatted information about it. Otherwise, the api will return the formatted information about all the tables inside the specified database. The extra information the get formatted table api is returned in the "extraInfo" field

Example:

    [
        {
            "database": "default",
            "tables": [
                {
                    "name": "x",
                    "columns": [
                        {
                            "name": "id",
                            "dataType": "string",
                            "comment": "None",
                            "members": []
                        }
                    ],
                    "extraInfo": [
                        [
                            "# Detailed Table Information",
                            "",
                            ""
                        ],
                        [
                            "Database:",
                            "default",
                            ""
                        ],
                        [
                            "Owner:",
                            "ubuntu",
                            ""
                        ],
                        [
                            "CreateTime:",
                            "Wed Apr 01 11:11:54 EEST 2015",
                            ""
                        ],
                        [
                            "LastAccessTime:",
                            "UNKNOWN",
                            ""
                        ],
                        [
                            "Protect Mode:",
                            "None",
                            ""
                        ],
                        [
                            "Retention:",
                            "0",
                            ""
                        ],
                        [
                            "Location:",
                            "hdfs://main:8020/user/hive/warehouse/x",
                            ""
                        ],
                        [
                            "Table Type:",
                            "MANAGED_TABLE",
                            ""
                        ],
                        [
                            "Table Parameters:",
                            "",
                            ""
                        ],
                        [
                            "",
                            "COLUMN_STATS_ACCURATE",
                            "true"
                        ],
                        [
                            "",
                            "numFiles",
                            "1"
                        ],
                        [
                            "",
                            "numRows",
                            "3"
                        ],
                        [
                            "",
                            "rawDataSize",
                            "192"
                        ],
                        [
                            "",
                            "totalSize",
                            "195"
                        ],
                        [
                            "",
                            "transient_lastDdlTime",
                            "1427875914"
                        ],
                        [
                            "",
                            "",
                            ""
                        ],
                        [
                            "# Storage Information",
                            "",
                            ""
                        ],
                        [
                            "SerDe Library:",
                            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                            ""
                        ],
                        [
                            "InputFormat:",
                            "org.apache.hadoop.mapred.TextInputFormat",
                            ""
                        ],
                        [
                            "OutputFormat:",
                            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                            ""
                        ],
                        [
                            "Compressed:",
                            "No",
                            ""
                        ],
                        [
                            "Num Buckets:",
                            "-1",
                            ""
                        ],
                        [
                            "Bucket Columns:",
                            "[]",
                            ""
                        ],
                        [
                            "Sort Columns:",
                            "[]",
                            ""
                        ],
                        [
                            "Storage Desc Params:",
                            "",
                            ""
                        ],
                        [
                            "",
                            "serialization.format",
                            "1"
                        ]
                    ]
                }
            ]
        }
    ]


# jaws-hive-sql-rest

A restful (http) interface that can concurrently and asynchronously submit HiveQL queries on top of hive cli, the results (automatically limited in size), execution logs and query history are persisted and vizible through Jaws's apis rescribed above.

## Building Jaws hive sql

You need to have maven installed.
To build, follow the steps below:

    cd jaws-hive-sql-rest/
    mvn clean install -DskipTests
    cd target
    tar -zxvf jaws-hive-sql-rest.tar.gz
    
## Configure Jaws hive sql

To configure the jaws-hive-sql-rest you have to edit the following files inside the conf folder.

    * application.conf: Contains all the application configurations. You will the details for each property inside the file
    * log4j.properties : Used for configuring the logger
    

## Run Jaws hive sql

After editing all the configuration files, run jaws-hive-sql-rest in the following manner:

    1. go where you decompressed the tar.gz file:
       cd jaws-hive-sql-rest/
    2. run:
       nohup bin/start-jaws-hive.sh &
    

## Apis

### Verify that Jaws hive sql is up

    curl 'devbox.local:7080/jaws/hive/index' -X GET
    
Results:
You will receive the following confirmation message that Jaws hive sql is up and running:

"Jaws hive sql rest is up and running!"

### Run query

    curl -d "show databases" 'http://devbox.local:7080/jaws/hive/run?limit=100' -X POST
    

Parameters:

  * limit [ not required] : limits the query's results

Results:

The api returns an uuid representing the query that was submitted. The query is executed asynchronously and the expectation is that clients poll for logs and completion status.

Example: 1404998257416357bb29d-6801-41ca-82e4-7a265816b50c
