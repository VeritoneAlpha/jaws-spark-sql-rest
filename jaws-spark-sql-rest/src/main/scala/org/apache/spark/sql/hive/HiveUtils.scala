package org.apache.spark.sql.hive

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, ObjectInputStream, ObjectOutputStream}
import java.util.regex.{Matcher, Pattern}
import com.xpatterns.jaws.data.DTO.{ParquetTable, QueryMetaInfo}
import com.xpatterns.jaws.data.contracts.{DAL, TJawsLogging}
import com.xpatterns.jaws.data.utils.{ResultsConverter, Utils}
import customs.CustomIndexer
import implementation.HiveContextWrapper
import org.apache.commons.lang.{RandomStringUtils, StringUtils}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.parquet.SparkParquetUtility._
import org.apache.spark.sql.types._
import server.LogsActor.PushLogs
import server.{Configuration, MainActors}

/**
 * Created by emaorhian
 */

object HiveUtils {
  val COLUMN_SEPARATOR = "###"

  def parseHql(hql: String): List[String] = {
    var result = List[String]()
    Configuration.log4j.info("[HiveUtils]: processing the following script:" + hql)
    var command: String = ""
    Option(hql) match {
      case None => Configuration.log4j.info("[HiveUtils] hql is null")
      case _ => {
        hql.split(";").foreach(oneCmd => {
          val trimmedCmd = oneCmd.trim()
          if (trimmedCmd.endsWith("\\")) {
            command += StringUtils.chop(trimmedCmd) + ";"
          } else {
            command += trimmedCmd
          }

          if (!StringUtils.isBlank(command)) {
            result = result ::: List(command)
          }

          command = ""

        })
      }
    }

    result
  }

  def runCmdRdd(cmd: String,
                hiveContext: HiveContextWrapper,
                defaultNumberOfResults: Int,
                uuid: String,
                isLimited: Boolean,
                maxNumberOfResults: Long,
                isLastCommand: Boolean,
                hdfsNamenode: String,
                loggingDal: TJawsLogging,
                conf: HadoopConfiguration,
                rddDestination: String): ResultsConverter = {

    Configuration.log4j.info("[HiveUtils]: execute the following command:" + cmd)

    val cmd_trimmed = cmd.trim
    val tokens = cmd_trimmed.split("\\s+")

    val (firstToken, secondToken) = if (tokens.size >= 2) (tokens(0).trim.toLowerCase(), tokens(1).trim.toLowerCase()) else (tokens(0).trim.toLowerCase(), "")

    (firstToken, secondToken) match {
      case ("select", _) => {

        Configuration.log4j.info("[HiveUtils]: the command is a select")
        Configuration.log4j.info("[HiveUtils]: the default number of results is: " + defaultNumberOfResults + " while the maximum number of results is " + maxNumberOfResults)
        executeSelect(cmd_trimmed, uuid, isLimited, maxNumberOfResults, isLastCommand, defaultNumberOfResults, hiveContext, loggingDal, hdfsNamenode, rddDestination, conf)
      }
      case ("set", _) => {
        // we won't put an uuid because it fails otherwise
        Configuration.log4j.info("[HiveUtils]: the command is a set")
        loggingDal.setRunMetaInfo(uuid, new QueryMetaInfo(0, maxNumberOfResults, 0, isLimited))
        null
      }

      case ("add", "jar") if (tokens.length >= 3) => {
        Configuration.log4j.info("[HiveUtils]: the command is a add jar")
        val jarPath = tokens(2).trim
        Configuration.log4j.info("[HiveUtils]: the jar to be added is" + jarPath)
        val resultSet = hiveContext.getSparkContext.addJar(jarPath)

        loggingDal.setRunMetaInfo(uuid, new QueryMetaInfo(0, maxNumberOfResults, 0, isLimited))
        null
      }
      case ("add", "file") if (tokens.length >= 3) => {
        Configuration.log4j.info("[HiveUtils]: the command is a add file")
        val filePath = tokens(2).trim
        Configuration.log4j.info("[HiveUtils]: the file to be added is" + filePath)
        val resultSet = hiveContext.getSparkContext.addFile(filePath)

        loggingDal.setRunMetaInfo(uuid, new QueryMetaInfo(0, maxNumberOfResults, 0, isLimited))
        null
      }

      case ("drop", _) | ("show", _) | ("describe", _) => {
        Configuration.log4j.info("[HiveUtils]: the command is a metadata query : " + tokens(0))
        val metadataResults = runMetadataCmd(hiveContext, cmd_trimmed)
        val rows = metadataResults map (arr => Row.fromSeq(arr))
        val result = new ResultsConverter(new StructType(Array(new StructField("result", StringType, true))), rows)
        loggingDal.setRunMetaInfo(uuid, new QueryMetaInfo(result.result.length, maxNumberOfResults, 0, isLimited))
        result
      }

      case _ => {
        Configuration.log4j.info("[HiveUtils]: the command is a different command")
        run(hiveContext, cmd_trimmed, maxNumberOfResults, isLimited, loggingDal, uuid)
      }

    }

  }

  def executeSelect(cmd: String,
                    uuid: String,
                    isLimited: Boolean,
                    maxNumberOfResults: Long,
                    isLastCommand: Boolean,
                    defaultNumberOfResults: Int,
                    hiveContext: HiveContextWrapper,
                    loggingDal: TJawsLogging,
                    hdfsNamenode: String,
                    rddDestination: String,
                    conf: HadoopConfiguration): ResultsConverter = {

    if (isLimited && maxNumberOfResults <= defaultNumberOfResults) {
      // ***** limited and few results -> cassandra
      val limitedCommand = limitQuery(maxNumberOfResults, cmd)
      run(hiveContext, limitedCommand, maxNumberOfResults, isLimited, loggingDal, uuid)
    } else if (isLimited && isLastCommand) {
      // ***** limited, lots of results and last command -> hdfs
      val limitedCommand = limitQuery(maxNumberOfResults, cmd)
      runRdd(hiveContext, uuid, limitedCommand, hdfsNamenode, maxNumberOfResults, isLimited, loggingDal, conf, rddDestination)
    } else if (isLimited) {
      // ***** limited, lots of results and not last command -> default
      // number and cassandra
      val limitedCommand = limitQuery(defaultNumberOfResults, cmd)
      run(hiveContext, limitedCommand, maxNumberOfResults, isLimited, loggingDal, uuid)
    } else if (isLastCommand) {
      // ***** not limited and last command -> hdfs and not a limit
      runRdd(hiveContext, uuid, cmd, hdfsNamenode, maxNumberOfResults, isLimited, loggingDal, conf, rddDestination)
    } else {
      // ***** not limited and not last command -> cassandra and default
      val limitedCommand = limitQuery(defaultNumberOfResults, cmd)
      run(hiveContext, limitedCommand, maxNumberOfResults, isLimited, loggingDal, uuid)
    }
  }

  def runMetadataCmd(hiveContext: HiveContextWrapper, cmd: String) = {
    val result = hiveContext.runMetadataSql(cmd).map(element => {
      if (element != null) {
        element.split("\t")
      } else Array(element)
    }).toArray
    result map (row => row map (_ trim ()))
  }

  def runRdd(hiveContext: HiveContext,
             uuid: String,
             cmd: String,
             destinationIp: String,
             maxNumberOfResults: Long,
             isLimited: Boolean,
             loggingDal: TJawsLogging,
             conf: HadoopConfiguration,
             userDefinedDestination: String): ResultsConverter = {

    // we need to run sqlToRdd. Needed for pagination
    Configuration.log4j.info("[HiveUtils]: we will execute sqlToRdd command")
    Configuration.log4j.info("[HiveUtils]: the final command is " + cmd)

    // select rdd
    val selectRdd = hiveContext.sql(cmd)
    val nbOfResults = selectRdd.count()

    // change the row into an array -> for serialization purpose
    val transformedSelectRdd = selectRdd.map(row => row.toSeq.toArray)

    val customIndexer = new CustomIndexer()
    val indexedRdd = customIndexer.indexRdd(transformedSelectRdd)

    // write schema on hdfs 
    Utils.rewriteFile(serializaSchema(selectRdd.schema), conf, Configuration.schemaFolder.getOrElse("jawsSchemaFolder") + "/" + uuid)
    var destination = getHdfsPath(destinationIp)

    userDefinedDestination.toLowerCase match {
      case "hdfs" => loggingDal.setRunMetaInfo(uuid, new QueryMetaInfo(nbOfResults, maxNumberOfResults, 1, isLimited))

      case "tachyon" =>
        loggingDal.setRunMetaInfo(uuid, new QueryMetaInfo(nbOfResults, maxNumberOfResults, 2, isLimited))
        destination = getTachyonPath(destinationIp)

      case _ => loggingDal.setRunMetaInfo(uuid, new QueryMetaInfo(nbOfResults, maxNumberOfResults, 1, isLimited))
    }

    // save rdd on hdfs
    indexedRdd.saveAsObjectFile(getRddDestinationPath(uuid, destination))
    null
  }

  def serializaSchema(schema: StructType): Array[Byte] = {

    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(schema)
    oos.close()
    baos.toByteArray
  }

  def deserializaSchema(schemaByteArray: Array[Byte]): StructType = {

    val ois = new ObjectInputStream(new ByteArrayInputStream(schemaByteArray))
    val schema = ois.readObject().asInstanceOf[StructType]
    ois.close()
    schema
  }

  def getRddDestinationPath(uuid: String, rddDestination: String): String = {
    var finalDestination = rddDestination
    if (!rddDestination.endsWith("/")) {
      finalDestination = rddDestination + "/"
    }
    s"${finalDestination}user/${System.getProperty("user.name")}/${Configuration.resultsFolder.getOrElse("jawsResultsFolder")}/$uuid"
  }

  def run(hiveContext: HiveContext,
          cmd: String,
          maxNumberOfResults: Long,
          isLimited: Boolean,
          loggingDal: TJawsLogging,
          uuid: String): ResultsConverter = {
    Configuration.log4j.info("[HiveUtils]: the final command is " + cmd)
    val resultRdd = hiveContext.sql(cmd)
    val result = resultRdd.collect
    val schema = resultRdd.schema
    loggingDal.setRunMetaInfo(uuid, new QueryMetaInfo(result.length, maxNumberOfResults, 0, isLimited))
    new ResultsConverter(schema, result)
  }

  def limitQuery(numberOfResults: Long, cmd: String): String = {
    val temporaryTableName = RandomStringUtils.randomAlphabetic(10)
    // take only x results
    "select " + temporaryTableName + ".* from ( " + cmd + ") " + temporaryTableName + " limit " + numberOfResults
  }

  def setSharkProperties(sc: HiveContext, sharkSettings: InputStream) = {

    scala.io.Source.fromInputStream(sharkSettings).getLines().foreach { line =>
      {
        sc.sql(line.trim())
      }
    }
  }

  //hardcoded ports - shouldn't they be taken from configuration?
  def getHdfsPath(ip: String): String = {
    "hdfs://" + ip.trim() + ":8020"
  }

  def getTachyonPath(ip: String): String = {
    "tachyon://" + ip.trim() + ":19998"
  }

  def logInfoMessage(uuid: String, message: String, jobId: String, loggingDal: TJawsLogging) {
    Configuration.log4j.info(message)
    logMessage(uuid, message, jobId, loggingDal)
  }

  def logMessage(uuid: String, message: String, jobId: String, loggingDal: TJawsLogging) {
    loggingDal.addLog(uuid, jobId, System.currentTimeMillis(), message)
    MainActors.logsActor ! new PushLogs(uuid, message)
  }


  def splitPath(filePath: String): (String, String) = {
    val pattern: Pattern = Pattern.compile("^([^/]+://[^/]+)(.+?)/*$")
    val matcher: Matcher = pattern.matcher(filePath)

    if (matcher.matches())
      (matcher.group(1), matcher.group(2))
    else
      throw new Exception(s"Invalid file path format : $filePath")

  }

  /**
    * Uses Parquet utility to register a table.
    *
    * @param hiveContext
    * @param tableName
    * @param parquetNamenode
    * @param tablePath
    * @param dals
    */
  def registerParquetTable(hiveContext: HiveContextWrapper,
                           tableName: String,
                           parquetNamenode: String,
                           tablePath: String,
                           dals: DAL) {
    Configuration.log4j.info(s"[HiveUtils] registering table $tableName")
    val parquetFile = hiveContext.readXPatternsParquet(parquetNamenode, tablePath)

    // register table
    parquetFile.registerTempTable(tableName)
    dals.parquetTableDal.addParquetTable(new ParquetTable(tableName, tablePath, parquetNamenode))
  }

  /**
    * Helper class that adds a more public method to unregister a table in Hive.
    * TableIdentifier is available only in org.apache.spark.sql package and included packages.
    *
    * @param hiveC
    */
  implicit class XPatternsHive (hiveC: HiveContextWrapper) {
    def unregisterTable(tableName: String): Unit = {
      hiveC.getCatalog.unregisterTable(TableIdentifier(tableName));
    }
  }
}