# http-spark-sql-server

http shark server for running Shark queries on top of Spark, with Mesos and Tachyon support (codenamed jaws)




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