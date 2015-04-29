# hdfs-tachyon-file-browser

Restful service for browsing files from HDFS or Tachyon. 

## Building the file browser

You need to have maven installed.
To build, follow the steps below:

    cd hdfs-tachyon-file-browser/
    mvn clean install -DskipTests
    cd target
    tar -zxvf hdfs-tachyon-file-browser.tar.gz
    
## Configure the file browser

To configure the hdfs-tachyon-file-browser you have to edit the following files inside the conf folder.

    * application.conf: Contains all the application configurations.
    * log4j.properties : Used for configuring the logger
    
The application.conf file contains two configurations: one for tunning spray (spray.can.server) and one for setting application preferences (appConf). You will find more details for each property inside the file.

## Run the file browser

After editing all the configuration files, run the file browser in the following manner:

    1. go where you decompressed the tar.gz file:
       cd hdfs-tachyon-file-browser/
    2. run:
       nohup bin/start-file-browser.sh &
    

## Apis

### Verify that the file browser is up

    curl 'devbox.local:8181/file-browser/index' -X GET
    
Results:
You will receive the following confirmation message that the hdfs-tachyon-file-browser is up and running:

"File browser is up and running!"

### List files

    curl 'http://devbox.local:8181/file-browser/listFiles?routePath=tachyon://devbox.local:19998/user/ubuntu' -X GET
    
    curl 'http://devbox.local:8181/file-browser/listFiles?routePath=hdfs://devbox.local:19998/user/ubuntu' -X GET
    

Parameters:

  * routePath [required] : the path to the hdfs/tachyon folder you want to list the files for

Results:

The api returns the list of subfolders and files for the provided directory. For each entry, there is a "isFolder" flag to specify if the entry is a folder or a file.

Exemple:

    {
      "parentPath": "/user/ubuntu",
      "files": [{
        "name": "folder1",
        "isFolder": true
      }, {
        "name": "folder2",
        "isFolder": true
      }, {
        "name": "file1",
        "isFolder": false
      }]
    }

