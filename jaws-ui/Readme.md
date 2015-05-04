# JawsUI
JawsUI is a user interface for Jaws

## Build
If you want to build the app yourself, you will need to perform the following tasks (**npn**, **bower** and **grunt-cli** must be installed previously) :
* Install your local project's **npn** tools. Running the following command will also install the required **bower** components
```bash
npm install
```
* Then run the **grunt** task. If everything goes well, the production version should be located in the **dist** folder:
```bash
grunt build
```

## Setup
To configure the app, edit the **config.js** file located in the application folder. The following parameters can be set:
* **COLOR_PALLETE**: Theme used by app. Valid palettes include: red, pink, purple, deep-purple, indigo, blue, light-blue, cyan, teal, green, light-green, lime, yellow, amber, orange, deep-orange, brown, grey, blue-grey
* **API_PATH**: The url path for the [Jaws] service
* **FILEBROWSE_API_PATH**:  The url path for the [hdfs-tachyon-file-browser] service
* **TACHYON_LOCATION**: The endpoint for Tachyon filesystem
* **HDFS_LOCATION**: The endpoint for HDFS filesystem
* **LOGS_PER_PAGE**: The number of logs to ask from the service per HTTP request
* **RESULTS_PER_PAGE**: The number of results to ask from the service per HTTP request
* **QUERIES_PER_PAGE**: The number of query history items to ask from the service per HTTP request