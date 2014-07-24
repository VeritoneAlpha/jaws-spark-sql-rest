package org.apache.spark.scheduler

import traits.DAL
import actors.Systems
import actors.MainActors
import shark.SharkContext
import com.xpatterns.jaws.data.DTO.ResultDTO
import actors.Configuration
import actors.LogsActor.PushLogs
import com.xpatterns.jaws.data.DTO.Result
import org.apache.commons.lang.time.DurationFormatUtils
import com.xpatterns.jaws.data.utils.QueryState
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by emaorhian
 */
class RunSharkScriptTask(dals: DAL, hqlScript: String, hiveContext: HiveContext, uuid: String, var isCanceled: Boolean, isLimited: Boolean, maxNumberOfResults: Long, hdfsConf: org.apache.hadoop.conf.Configuration) extends Runnable with MainActors with Systems {

  override def run() {
    try {
      dals.loggingDal.setState(uuid, QueryState.IN_PROGRESS)
      dals.loggingDal.setScriptDetails(uuid, hqlScript)

      // parse the hql into independent commands
      val commands = SharkUtils.parseHql(hqlScript)
      var result: Result = null
      val nrOfCommands = commands.size

      var message = "There are " + nrOfCommands + " commands that need to be executed"
      Configuration.log4j.info(message)
      dals.loggingDal.addLog(uuid, "hql", System.currentTimeMillis(), message)
      logsActor ! new PushLogs(uuid, message)

      val startTime = System.currentTimeMillis()

      // job group id used to identify these jobs when trying to cancel them.
      hiveContext.sparkContext.setJobGroup(uuid, "")

      // run each command except the last one
      for (commandIndex <- 0 to nrOfCommands - 2) {
        isCanceled match {
          case false => result = runCommand(commands(commandIndex), nrOfCommands, commandIndex, isLimited, false)
          case _ => Configuration.log4j.info("The task " + uuid + " was canceled")
        }
      }

      // the last command might need to be paginated
      isCanceled match {
        case false => result = runCommand(commands(nrOfCommands - 1), nrOfCommands, nrOfCommands - 1, isLimited, true)
        case _ => Configuration.log4j.info("The task " + uuid + " was canceled")
      }

      val executionTime = System.currentTimeMillis() - startTime
      var formattedDuration = DurationFormatUtils.formatDurationHMS(executionTime)

      Option(result) match {
        case None => Configuration.log4j.info("[RunSharkScriptTask] result is null")
        case _ => dals.resultsDal.setResults(uuid, result.toDTO)
      }

      message = "The total execution time was: " + formattedDuration + "!"
      dals.loggingDal.addLog(uuid, "hql", System.currentTimeMillis(), message)
      logsActor ! new PushLogs(uuid, message)
      dals.loggingDal.setState(uuid, QueryState.DONE)

    } catch {
      case e: Exception => {
        Configuration.log4j.error(e.getMessage())
        throw new RuntimeException(e)
      }
    }
  }

  def runCommand(command: String, nrOfCommands: Integer, commandIndex: Integer, isLimited: Boolean, isLastCommand: Boolean): Result = {
    var message = ""

    try {
      val result = HiveUtils.runCmdRdd(command, hiveContext, Configuration.numberOfResults.getOrElse("100").toInt, uuid, isLimited, maxNumberOfResults, isLastCommand, Configuration.jawsNamenode.get, dals.loggingDal, hdfsConf)
      message = "Command progress : There were executed " + (commandIndex + 1) + " commands out of " + nrOfCommands
      Configuration.log4j.info(message)
      dals.loggingDal.addLog(uuid, "hql", System.currentTimeMillis(), message)
      logsActor ! PushLogs(uuid, message)
      return result
    } catch {
      case e: Exception => {
        Configuration.log4j.error(e.getMessage())
        dals.loggingDal.addLog(uuid, "hql", System.currentTimeMillis(), e.getMessage())
        logsActor ! PushLogs(uuid, e.getMessage())
        dals.loggingDal.setState(uuid, QueryState.FAILED)
        throw new RuntimeException(e)
      }
    }
  }

  def setCanceled(canceled: Boolean) {
    isCanceled = canceled
  }

}