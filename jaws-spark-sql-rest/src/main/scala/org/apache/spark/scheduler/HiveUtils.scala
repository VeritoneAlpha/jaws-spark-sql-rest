package org.apache.spark.scheduler

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.lang.StringUtils
import com.xpatterns.jaws.data.DTO.ScriptMetaDTO
import com.xpatterns.jaws.data.contracts.IJawsLogging
import com.xpatterns.jaws.data.utils.Utils
import actors.Configuration
import actors.MainActors
import actors.Systems
import customs.CustomIndexer
import customs.CustomIndexer
import com.xpatterns.jaws.data.DTO.Result
import shark.api.ResultSet
import shark.api.TableRDD
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by emaorhian
 */
class HiveUtils extends MainActors with Systems {

}

object HiveUtils {
  val COLUMN_SEPARATOR = "###"

//  def parseHql(hql: String): List[String] = {
//    var result = List[String]()
//    Configuration.log4j.info("[SharkUtils]: processing the following script:" + hql)
//    var command: String = ""
//    Option(hql) match {
//      case None => Configuration.log4j.info("[SharkUtils] hql is null")
//      case _ => {
//        hql.split(";").foreach(oneCmd => {
//          val trimmedCmd = oneCmd.trim()
//          if (trimmedCmd.endsWith("\\")) {
//            command += StringUtils.chop(trimmedCmd) + ";"
//          } else {
//            command += trimmedCmd
//          }
//
//          if (StringUtils.isBlank(command) == false) {
//            result = result ::: List(command)
//          }
//
//          command = ""
//
//        })
//      }
//    }
//
//    return result
//  }
//
//  
//  def runCmd(cmd: String, hiveContext: HiveContext, uuid: String, loggingDal: IJawsLogging): Result = {
//    Configuration.log4j.info("[SharkUtils]: execute the following command:" + cmd)
//    val prefix = "--" + uuid + "\n"
//    var cmd_trimmed: String = cmd.trim()
//    val tokens = cmd_trimmed.split("\\s+")
//
//    if (tokens(0).equalsIgnoreCase("select")) {
//      Configuration.log4j.info("[SharkUtils]: the command is a select")
//      Configuration.log4j.info("[SharkUtils]: the maximum number of results is: " + Configuration.numberOfResults.getOrElse("100"))
//      val temporaryTableName = RandomStringUtils.randomAlphabetic(10)
//      // take only x results
//      cmd_trimmed = "select " + temporaryTableName + ".* from ( " + cmd_trimmed + ") " + temporaryTableName + " limit " +  Configuration.numberOfResults.getOrElse("100")
//    } else if (tokens(0).equalsIgnoreCase("set") || tokens(0).equalsIgnoreCase("add")) {
//      // we won't put an uuid
//      Configuration.log4j.info("[SharkUtils]: the command is a set or add")
//      hiveContext.hql("")
//      val resultSet = hiveContext.hql(cmd_trimmed).collect
////      return Result.fromResultSet(resultSet)
//      return null
//    }
//
//    cmd_trimmed = prefix + cmd_trimmed
//    Configuration.log4j.info("[SharkUtils]: the command is a different command")
//    return run(hiveContext, cmd_trimmed, 0, false, loggingDal, uuid)
//  }

  def runCmdRdd(cmd: String, hiveContext: HiveContext, defaultNumberOfResults: Int, uuid: String, isLimited: Boolean, maxNumberOfResults: Long, isLastCommand: Boolean, hdfsNamenode: String, loggingDal: IJawsLogging, conf: org.apache.hadoop.conf.Configuration): Result = {
    Configuration.log4j.info("[SharkUtils]: execute the following command:" + cmd)
    val prefix = "--" + uuid + "\n"
    var cmd_trimmed = cmd.trim()
    val tokens = cmd_trimmed.split("\\s+")

    if (tokens(0).equalsIgnoreCase("select")) {

      Configuration.log4j.info("[SharkUtils]: the command is a select")
      Configuration.log4j.info("[SharkUtils]: the default number of results is: " + defaultNumberOfResults + " while the maximum number of results is " + maxNumberOfResults)

      // ***** limited and few results -> cassandra
      if (isLimited && maxNumberOfResults <= defaultNumberOfResults) {
        cmd_trimmed = limitQuery(maxNumberOfResults, cmd_trimmed)
        cmd_trimmed = prefix + cmd_trimmed
        return run(hiveContext, cmd_trimmed, maxNumberOfResults, isLimited, loggingDal, uuid)
      }
      // ***** limited, lots of results and last command -> hdfs
      if (isLimited && isLastCommand) {
        cmd_trimmed = limitQuery(maxNumberOfResults, cmd_trimmed)
        cmd_trimmed = prefix + cmd_trimmed
        return runRdd(hiveContext, uuid, cmd_trimmed, hdfsNamenode, maxNumberOfResults, isLimited, loggingDal, conf)
      }

      // ***** limited, lots of results and not last command -> default
      // number and cassandra
      if (isLimited) {
        cmd_trimmed = limitQuery(defaultNumberOfResults, cmd_trimmed)
        cmd_trimmed = prefix + cmd_trimmed
        return run(hiveContext, cmd_trimmed, maxNumberOfResults, isLimited, loggingDal, uuid)
      }

      // ***** not limited and last command -> hdfs and not a limit
      if (isLastCommand) {
        cmd_trimmed = prefix + cmd_trimmed
        return runRdd(hiveContext, uuid, cmd_trimmed, hdfsNamenode, maxNumberOfResults, isLimited, loggingDal, conf)
      }
      // ***** not limited and not last command -> cassandra and default
      cmd_trimmed = limitQuery(defaultNumberOfResults, cmd_trimmed)
      cmd_trimmed = prefix + cmd_trimmed
      return run(hiveContext, cmd_trimmed, maxNumberOfResults, isLimited, loggingDal, uuid)

    }

    if (tokens(0).equalsIgnoreCase("set") || tokens(0).equalsIgnoreCase("add")) {
      // we won't put an uuid
      Configuration.log4j.info("[SharkUtils]: the command is a set or add")
      val resultSet = hiveContext.sql(cmd_trimmed)
      return null
    }

    cmd_trimmed = prefix + cmd_trimmed
    Configuration.log4j.info("[SharkUtils]: the command is a different command")
    return run(hiveContext, cmd_trimmed, maxNumberOfResults, isLimited, loggingDal, uuid)
  }
//
//  def runRdd(hiveContext: HiveContext, uuid: String, cmd: String, hdfsNamenode: String, maxNumberOfResults: Long, isLimited: Boolean, loggingDal: IJawsLogging, conf: org.apache.hadoop.conf.Configuration): Result = {
//    // we need to run sqlToRdd. Needed for pagination
//    Configuration.log4j.info("[SharkUtils]: we will execute sqlToRdd command")
//    Configuration.log4j.info("[SharkUtils]: the final command is " + cmd)
//
//    // select rdd
//    val selectRdd = hiveContext.hql(cmd)
//    val nbOfResults = selectRdd.count()
//
//    // change the shark Row into Object[] -> for serialization purpose
//    val transformedSelectRdd = selectRdd.map(row => {
//      val resultSize = row.colname2indexMap.size
//      val result = new Array[Object](resultSize)
//      var index = 0
//      while (index < resultSize) {
//        result(index) = row.get(index)
//        index += 1
//      }
//      result
//    })
//
//    val customIndexer = new CustomIndexer()
//    val indexedRdd = customIndexer.indexRdd(transformedSelectRdd)
//
//    loggingDal.setMetaInfo(uuid, new ScriptMetaDTO(nbOfResults, maxNumberOfResults, false, isLimited))
//
//    // write schema on hdfs
//    Utils.rewriteFile(getSchema(selectRdd), conf, Configuration.schemaFolder.getOrElse("jawsSchemaFolder") + "/" + uuid)
//    // save rdd on hdfs
//    indexedRdd.saveAsObjectFile(getHDFSRddPath(uuid, hdfsNamenode))
//    return null
//  }
//
//  def getSchema(selectRdd: TableRDD): String = {
//    var schema = ""
//    selectRdd.schema.foreach(column => {
//      schema = schema + column.name + COLUMN_SEPARATOR
//    })
//    val lastSeparator = schema.lastIndexOf(COLUMN_SEPARATOR)
//    schema.substring(0, lastSeparator)
//
//  }
//
//  def getHDFSRddPath(uuid: String, hdfsNamenode: String): String = {
//    var returnHdfsNamenode = hdfsNamenode
//    if (hdfsNamenode.endsWith("/") == false) {
//      returnHdfsNamenode = hdfsNamenode + "/"
//    }
//
//    return returnHdfsNamenode + "user/" + System.getProperty("user.name") + "/" + uuid
//  }
//
  def run(HiveContext: HiveContext, cmd: String, maxNumberOfResults: Long, isLimited: Boolean, loggingDal: IJawsLogging, uuid: String): Result = {
    Configuration.log4j.info("[SharkUtils]: the final command is " + cmd)
    val resultRdd = HiveContext.hql(cmd)
    val result = resultRdd.collect
    val schema = resultRdd.queryExecution.analyzed.outputSet
    loggingDal.setMetaInfo(uuid, new ScriptMetaDTO(result.size, maxNumberOfResults, true, isLimited))
    return Result(schema, resultSet)
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

//  def getSchema(schemaString: String): List[String] = {
//    return schemaString.split(COLUMN_SEPARATOR).toList
//  }

}