package org.apache.spark.scheduler

import com.xpatterns.jaws.data.contracts.DAL
import org.apache.hadoop.conf.{ Configuration => HadoopConfiguration }
import com.xpatterns.jaws.data.utils.Utils._
import server.MainActors
import server.Configuration
import server.LogsActor.PushLogs
import com.xpatterns.jaws.data.DTO.Result
import org.apache.commons.lang.time.DurationFormatUtils
import com.xpatterns.jaws.data.utils.QueryState
import implementation.HiveContextWrapper
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import org.apache.spark.sql.parquet.ParquetUtils._
import com.xpatterns.jaws.data.DTO.ParquetTable
import akka.actor.ActorSelection
import akka.pattern.ask
import messages.RegisterTableMessage
import akka.util.Timeout
import scala.util.Success
import scala.util.Failure
import scala.concurrent.ExecutionContext.Implicits.global
import messages.ErrorMessage
import scala.util.Success
import server.JawsController
import scala.concurrent.Future
/**
 * Created by emaorhian
 */
class RunScriptTask(dals: DAL, hqlScript: String, hiveContext: HiveContextWrapper,
  uuid: String, @volatile var isCanceled: Boolean, isLimited: Boolean,
  maxNumberOfResults: Long, hdfsConf: HadoopConfiguration, rddDestination: String) extends Runnable {

  override def run() {
    try {
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
        var message = getCompleteStackTrace(e)
        Configuration.log4j.error(message)
        HiveUtils.logMessage(uuid, message, "hql", dals.loggingDal)
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

class RunParquetScriptTask(dals: DAL, hqlScript: String, hiveContext: HiveContextWrapper,
  uuid: String, isCanceled: Boolean, isLimited: Boolean, maxNumberOfResults: Long,
  hdfsConf: HadoopConfiguration, rddDestination: String, tableName: String, tablePath: String)
  extends RunScriptTask(dals, hqlScript, hiveContext, uuid, isCanceled, isLimited, maxNumberOfResults, hdfsConf, rddDestination) {

  override def run() {
    implicit val timeout = Timeout(Configuration.timeout.toInt)
    val future = ask(JawsController.balancerActor, RegisterTableMessage(tableName, tablePath))
      .map(innerFuture => innerFuture.asInstanceOf[Future[Any]])
      .flatMap(identity)

    future onComplete {
      case Success(x) => x match {
        case e: ErrorMessage => {
          HiveUtils.logMessage(uuid, e.message, "hql", dals.loggingDal)
          dals.loggingDal.setState(uuid, QueryState.FAILED)
        }
        case result: String => {
          Configuration.log4j.info(result)
          super.run
        }
      }
      case Failure(ex) => {
        HiveUtils.logMessage(uuid, getCompleteStackTrace(ex), "hql", dals.loggingDal)
        dals.loggingDal.setState(uuid, QueryState.FAILED)
      }
    }
  }

}