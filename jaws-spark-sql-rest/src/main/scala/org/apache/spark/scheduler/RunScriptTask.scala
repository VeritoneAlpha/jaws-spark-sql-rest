package org.apache.spark.scheduler

import traits.DAL
import server.MainActors
import server.Configuration
import server.LogsActor.PushLogs
import com.xpatterns.jaws.data.DTO.Result
import org.apache.commons.lang.time.DurationFormatUtils
import com.xpatterns.jaws.data.utils.QueryState
import implementation.HiveContextWrapper
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import org.apache.spark.sql.parquet.ParquetUtils._

/**
 * Created by emaorhian
 */
class RunScriptTask(dals: DAL, hqlScript: String, hiveContext: HiveContextWrapper, uuid: String, var isCanceled: Boolean, isLimited: Boolean, maxNumberOfResults: Long, hdfsConf: org.apache.hadoop.conf.Configuration, rddDestination: String) extends Runnable {

  override def run() {
    try {
      dals.loggingDal.setState(uuid, QueryState.IN_PROGRESS)
      dals.loggingDal.setScriptDetails(uuid, hqlScript)

      // parse the hql into independent commands
      val commands = HiveUtils.parseHql(hqlScript)
      var result: Result = null
      val nrOfCommands = commands.size

      HiveUtils.logInfoMessage(uuid, s"There are $nrOfCommands commands that need to be executed", "hql", dals.loggingDal)

      val startTime = System.currentTimeMillis()

      // job group id used to identify these jobs when trying to cancel them.
      hiveContext.sparkContext.setJobGroup(uuid, "")

      // run each command except the last one
      for (commandIndex <- 0 to nrOfCommands - 2) {
        isCanceled match {
          case false => {
            result = HiveUtils.runCmdRdd(commands(commandIndex), hiveContext, Configuration.numberOfResults.getOrElse("100").toInt, uuid, isLimited, maxNumberOfResults, false, Configuration.rddDestinationIp.get, dals.loggingDal, hdfsConf, rddDestination)
            HiveUtils.logInfoMessage(uuid, s"Command progress : There were executed ${commandIndex + 1} commands out of $nrOfCommands", "hql", dals.loggingDal)
          }
          case _ => {
            val message = s"The command ${commands(commandIndex)} was canceled!"
            Configuration.log4j.warn(message)
            HiveUtils.logMessage(uuid, message, "hql", dals.loggingDal)
          }
        }
      }

      // the last command might need to be paginated
      isCanceled match {
        case false => {
            result = HiveUtils.runCmdRdd(commands(nrOfCommands - 1), hiveContext, Configuration.numberOfResults.getOrElse("100").toInt, uuid, isLimited, maxNumberOfResults, true, Configuration.rddDestinationIp.get, dals.loggingDal, hdfsConf, rddDestination)
            HiveUtils.logInfoMessage(uuid, s"Command progress : There were executed $nrOfCommands commands out of $nrOfCommands", "hql", dals.loggingDal)
          }
        case _ => {
          val message = s"The command ${commands(nrOfCommands - 1)} was canceled!"
          Configuration.log4j.warn(message)
          HiveUtils.logMessage(uuid, message, "hql", dals.loggingDal)
        }
      }

      val executionTime = System.currentTimeMillis() - startTime
      var formattedDuration = DurationFormatUtils.formatDurationHMS(executionTime)

      HiveUtils.logInfoMessage(uuid, s"The total execution time was: $formattedDuration!", "hql", dals.loggingDal)
      writeResults(isCanceled, result)
    } catch {
      case e: Exception => {
        val message = s"${e.getMessage()} : ${e.getStackTraceString}"
        Configuration.log4j.error(message)
        HiveUtils.logMessage(uuid, e.getStackTraceString, "hql", dals.loggingDal)
        dals.loggingDal.setState(uuid, QueryState.FAILED)
        dals.loggingDal.setMetaInfo(uuid, new QueryMetaInfo(0, maxNumberOfResults, 0, isLimited))

        throw new RuntimeException(e)
      
      }
    }
  }

  def writeResults(isCanceled: Boolean, result: Result) {
    isCanceled match {
      case false => {
        Option(result) match {
          case None => Configuration.log4j.debug("[RunSharkScriptTask] result is null")
          case _ => dals.resultsDal.setResults(uuid, result)
        }
        dals.loggingDal.setState(uuid, QueryState.DONE)
      }
      case _ => {
        val message = s"The query failed because it was canceled!"
        Configuration.log4j.warn(message)
        HiveUtils.logMessage(uuid, message, "hql", dals.loggingDal)
        dals.loggingDal.setState(uuid, QueryState.FAILED)
      }
    }
  }

  def setCanceled(canceled: Boolean) {
    isCanceled = canceled
  }

}

class RunParquetScriptTask(dals: DAL, hqlScript: String, hiveContext: HiveContextWrapper, uuid: String, isCanceled: Boolean, isLimited: Boolean, maxNumberOfResults: Long, hdfsConf: org.apache.hadoop.conf.Configuration, rddDestination: String, tableName: String, parquetNamenode: String, tablePath: String)
  extends RunScriptTask(dals, hqlScript, hiveContext, uuid, isCanceled, isLimited, maxNumberOfResults, hdfsConf, rddDestination) {

  override def run() {
    val parquetFile = hiveContext.readXPatternsParquet(parquetNamenode, tablePath)

    // register table
    parquetFile.registerTempTable(tableName)
    super.run
  }

}