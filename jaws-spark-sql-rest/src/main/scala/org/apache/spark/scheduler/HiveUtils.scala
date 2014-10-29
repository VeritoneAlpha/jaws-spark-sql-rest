package org.apache.spark.scheduler

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.lang.StringUtils
import com.xpatterns.jaws.data.utils.Utils
import server.Configuration
import server.MainActors
import server.Systems
import customs.CustomIndexer
import customs.CustomIndexer
import com.xpatterns.jaws.data.DTO.Result
import org.apache.spark.sql.hive.HiveContext
import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import org.apache.spark.sql.catalyst.expressions.Attribute
import net.liftweb.json._
import spray.json._
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import implementation.HiveContextWrapper
import com.xpatterns.jaws.data.DTO.Column
import implementation.HiveContextWrapper

/**
 * Created by emaorhian
 */
class HiveUtils extends MainActors with Systems {

}

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

          if (StringUtils.isBlank(command) == false) {
            result = result ::: List(command)
          }

          command = ""

        })
      }
    }

    return result
  }

  def runCmdRdd(cmd: String, hiveContext: HiveContextWrapper, defaultNumberOfResults: Int, uuid: String, isLimited: Boolean, maxNumberOfResults: Long, isLastCommand: Boolean, hdfsNamenode: String, loggingDal: TJawsLogging, conf: org.apache.hadoop.conf.Configuration): Result = {
    Configuration.log4j.info("[HiveUtils]: execute the following command:" + cmd)

    var cmd_trimmed = cmd.trim
    val lowerCaseCmd = cmd_trimmed.toLowerCase()
    
    val tokens = cmd_trimmed.split("\\s+")

    if (tokens(0).equalsIgnoreCase("select")) {

      Configuration.log4j.info("[HiveUtils]: the command is a select")
      Configuration.log4j.info("[HiveUtils]: the default number of results is: " + defaultNumberOfResults + " while the maximum number of results is " + maxNumberOfResults)

      // ***** limited and few results -> cassandra
      if (isLimited && maxNumberOfResults <= defaultNumberOfResults) {
        cmd_trimmed = limitQuery(maxNumberOfResults, cmd_trimmed)
        return run(hiveContext, cmd_trimmed, maxNumberOfResults, isLimited, loggingDal, uuid)
      }
      // ***** limited, lots of results and last command -> hdfs
      if (isLimited && isLastCommand) {
        cmd_trimmed = limitQuery(maxNumberOfResults, cmd_trimmed)
        return runRdd(hiveContext, uuid, cmd_trimmed, hdfsNamenode, maxNumberOfResults, isLimited, loggingDal, conf)
      }

      // ***** limited, lots of results and not last command -> default
      // number and cassandra
      if (isLimited) {
        cmd_trimmed = limitQuery(defaultNumberOfResults, cmd_trimmed)
        return run(hiveContext, cmd_trimmed, maxNumberOfResults, isLimited, loggingDal, uuid)
      }

      // ***** not limited and last command -> hdfs and not a limit
      if (isLastCommand) {
        return runRdd(hiveContext, uuid, cmd_trimmed, hdfsNamenode, maxNumberOfResults, isLimited, loggingDal, conf)
      }
      // ***** not limited and not last command -> cassandra and default
      cmd_trimmed = limitQuery(defaultNumberOfResults, cmd_trimmed)
      return run(hiveContext, cmd_trimmed, maxNumberOfResults, isLimited, loggingDal, uuid)

    }

    if (tokens(0).equalsIgnoreCase("set")) {
      // we won't put an uuid because it fails otherwise
      Configuration.log4j.info("[HiveUtils]: the command is a set")
      val resultSet = hiveContext.hql(cmd_trimmed)
      loggingDal.setMetaInfo(uuid, new QueryMetaInfo(0, maxNumberOfResults, true, isLimited))
      return new Result(Array[Column](), Array[Array[String]] ())
    }
    
    if (tokens.size >= 3 && tokens(0).trim.equalsIgnoreCase("add") && tokens(1).trim.equalsIgnoreCase("jar")){
      Configuration.log4j.info("[HiveUtils]: the command is a add jar")
      val jarPath = tokens(2).trim
      Configuration.log4j.info("[HiveUtils]: the jar to be added is" + jarPath)
      val resultSet = hiveContext.getSparkContext.addJar(jarPath)
    
      loggingDal.setMetaInfo(uuid, new QueryMetaInfo(0, maxNumberOfResults, true, isLimited))
      return new Result(Array[Column](), Array[Array[String]] ())
    }
    
    if (tokens.size >= 3 && tokens(0).trim.equalsIgnoreCase("add") && tokens(1).trim.equalsIgnoreCase("file")){
      Configuration.log4j.info("[HiveUtils]: the command is a add file")
      val filePath = tokens(2).trim
      Configuration.log4j.info("[HiveUtils]: the file to be added is" + filePath)
      val resultSet = hiveContext.getSparkContext.addFile(filePath)
    
      loggingDal.setMetaInfo(uuid, new QueryMetaInfo(0, maxNumberOfResults, true, isLimited))
      return new Result(Array[Column](), Array[Array[String]] ())
    }
    

    if (tokens(0).equalsIgnoreCase("drop") || tokens(0).equalsIgnoreCase("show") || tokens(0).equalsIgnoreCase("describe")) {
      Configuration.log4j.info("[HiveUtils]: the command is a metadata query : " + tokens(0))
      
      val result = runMetadataCmd(hiveContext, cmd_trimmed, loggingDal, uuid)
      loggingDal.setMetaInfo(uuid, new QueryMetaInfo(result.results.size, maxNumberOfResults, true, isLimited))
      return result
    }

    
    
    Configuration.log4j.info("[HiveUtils]: the command is a different command")
    return run(hiveContext, cmd_trimmed, maxNumberOfResults, isLimited, loggingDal, uuid)
  }

  def runMetadataCmd(hiveContext: HiveContextWrapper, cmd: String, loggingDal: TJawsLogging, uuid: String): Result = {
    val result = hiveContext.runMetadataSql(cmd).map(element => {
      if (element != null) {
        element.split("\t")
      }
      else Array(element)
    }).toArray
    val schema = new Array[Column](1)
    schema(0) = new Column("result", "stringType")
    return new Result(schema, result)
  }

  def runRdd(hiveContext: HiveContext, uuid: String, cmd: String, hdfsNamenode: String, maxNumberOfResults: Long, isLimited: Boolean, loggingDal: TJawsLogging, conf: org.apache.hadoop.conf.Configuration): Result = {
    // we need to run sqlToRdd. Needed for pagination
    Configuration.log4j.info("[HiveUtils]: we will execute sqlToRdd command")
    Configuration.log4j.info("[HiveUtils]: the final command is " + cmd)

    // select rdd
    val selectRdd = hiveContext.hql(cmd)
    val nbOfResults = selectRdd.count()

    // change the shark Row into String[] -> for serialization purpose 
    val transformedSelectRdd = selectRdd.map(row => {

      var result = row.map(value => {
        Option(value) match {
          case None => "Null"
          case _ => value.toString()
        }
      })
      result.toArray
    })

    val customIndexer = new CustomIndexer()
    val indexedRdd = customIndexer.indexRdd(transformedSelectRdd)

    loggingDal.setMetaInfo(uuid, new QueryMetaInfo(nbOfResults, maxNumberOfResults, false, isLimited))

    // write schema on hdfs
    Utils.rewriteFile(getSchema(selectRdd.queryExecution.analyzed.outputSet), conf, Configuration.schemaFolder.getOrElse("jawsSchemaFolder") + "/" + uuid)
    // save rdd on hdfs
    indexedRdd.saveAsObjectFile(getHDFSRddPath(uuid, hdfsNamenode))
    return null
  }

  def getSchema(schema: Set[Attribute]): String = {
    val transformedSchema = Result.getSchema(schema)
    transformedSchema.toJson.toString()
  }

  def getHDFSRddPath(uuid: String, hdfsNamenode: String): String = {
    var returnHdfsNamenode = hdfsNamenode
    if (hdfsNamenode.endsWith("/") == false) {
      returnHdfsNamenode = hdfsNamenode + "/"
    }

    return returnHdfsNamenode + "user/" + System.getProperty("user.name") + "/" + Configuration.resultsFolder.getOrElse("jawsResultsFolder") + "/" + uuid
  }

  def run(hiveContext: HiveContext, cmd: String, maxNumberOfResults: Long, isLimited: Boolean, loggingDal: TJawsLogging, uuid: String): Result = {
    Configuration.log4j.info("[HiveUtils]: the final command is " + cmd)
    val resultRdd = hiveContext.hql(cmd)
    val result = resultRdd.collect
    val schema = resultRdd.queryExecution.analyzed.outputSet
    loggingDal.setMetaInfo(uuid, new QueryMetaInfo(result.size, maxNumberOfResults, true, isLimited))
    return new Result(schema, result)
  }

  def limitQuery(numberOfResults: Long, cmd: String): String = {
    val temporaryTableName = RandomStringUtils.randomAlphabetic(10)
    // take only x results
    return "select " + temporaryTableName + ".* from ( " + cmd + ") " + temporaryTableName + " limit " + numberOfResults
  }

  def setSharkProperties(sc: HiveContext, sharkSettings: InputStream) = {

    scala.io.Source.fromInputStream(sharkSettings).getLines().foreach { line =>
      {
        sc.hql(line.trim())
      }
    }
  }

}