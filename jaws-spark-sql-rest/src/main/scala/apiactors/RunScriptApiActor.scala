package apiactors

import akka.actor.Actor
import akka.actor.actorRef2Scala
import com.google.common.base.Preconditions
import traits.DAL
import messages.RunScriptMessage
import java.util.UUID
import server.Configuration
import com.google.common.cache.CacheBuilder
import java.util.concurrent.TimeUnit
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import server.MainActors
import com.google.common.cache.Cache
import messages.CancelMessage
import org.apache.spark.scheduler.RunScriptTask
import implementation.HiveContextWrapper
import messages.RunParquetMessage
import org.apache.spark.sql.parquet.ParquetUtils._
import java.util.regex.Pattern
import java.util.regex.Matcher
import scala.util.Try
import apiactors.ActorOperations._
import org.apache.spark.scheduler.RunParquetScriptTask
/**
 * Created by emaorhian
 */
class RunScriptApiActor(hdfsConf: org.apache.hadoop.conf.Configuration, hiveContext: HiveContextWrapper, dals: DAL) extends Actor {
  var taskCache: Cache[String, RunScriptTask] = _
  var threadPool: ThreadPoolTaskExecutor = _
  val pattern: Pattern = Pattern.compile("^([^/]+://[^/]+)(.+?)/*$")

  override def preStart() {
    taskCache = {
      CacheBuilder
        .newBuilder()
        .maximumSize(1000)
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build[String, RunScriptTask]
    }

    threadPool = new ThreadPoolTaskExecutor()
    threadPool.setCorePoolSize(Configuration.nrOfThreads.getOrElse("10").toInt)
    threadPool.initialize()
  }

  override def receive = {

    case message: RunScriptMessage => {

      val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
      val tryRun = Try {
        Configuration.log4j.info("[RunScriptApiActor -run]: running the following script: " + message.hqlScript)
        Configuration.log4j.info("[RunScriptApiActor -run]: The script will be executed with the limited flag set on " + message.limited + ". The maximum number of results is " + message.maxNumberOfResults)
        Preconditions.checkArgument(message.hqlScript != null && !message.hqlScript.isEmpty(), Configuration.SCRIPT_EXCEPTION_MESSAGE)
        Preconditions.checkArgument(message.limited != null, Configuration.LIMITED_EXCEPTION_MESSAGE)
        Preconditions.checkArgument(message.maxNumberOfResults != null, Configuration.RESULSTS_NUMBER_EXCEPTION_MESSAGE)

        val task = new RunScriptTask(dals, message.hqlScript, hiveContext, uuid, false, message.limited, message.maxNumberOfResults, hdfsConf, message.rddDestination)
        taskCache.put(uuid, task)
        threadPool.execute(task)
      }
      returnResult(tryRun, uuid, "run query failed with the following message: ", sender)
    }

    case message: RunParquetMessage => {
      val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
      val tryRunParquet = Try {
        Preconditions.checkArgument(message.script != null && !message.script.isEmpty(), Configuration.SCRIPT_EXCEPTION_MESSAGE)
        Preconditions.checkArgument(message.tablePath != null, Configuration.FILE_EXCEPTION_MESSAGE)
        Preconditions.checkArgument(message.table != null, Configuration.TABLE_EXCEPTION_MESSAGE)

        Configuration.log4j.info(s"[RunScriptApiActor -runParquet]: running the following sql: ${message.script}")
        Configuration.log4j.info(s"[RunScriptApiActor -runParquet]: The script will be executed over the ${message.tablePath} file with the ${message.table} table name")

        //load the parquet file
        val (namenode, folderPath) = splitPath(message.tablePath)
        Configuration.log4j.info(s"[RunScriptApiActor -runParquet]: namenode = $namenode, path = $folderPath ")      
       
        val task = new RunParquetScriptTask(dals, message.script, hiveContext, uuid, false, message.limited, message.maxNumberOfResults, hdfsConf, message.rddDestination, message.table, namenode, folderPath)
        taskCache.put(uuid, task)
        threadPool.execute(task)
      }
      returnResult(tryRunParquet, uuid, "run parquet query failed with the following message: ", sender)
    }

    case message: CancelMessage => {
      Configuration.log4j.info("[RunScriptApiActor]: Canceling the jobs for the following uuid: " + message.queryID)

      val task = taskCache.getIfPresent(message.queryID)

      Option(task) match {
        case None => {
          Configuration.log4j.info("No job to be canceled")
        }
        case _ => {

          task.setCanceled(true)
          taskCache.invalidate(message.queryID)

          if (Option(hiveContext.sparkContext.getConf.get("spark.mesos.coarse")).getOrElse("true").equalsIgnoreCase("true")) {
            Configuration.log4j.info("[RunScriptApiActor]: Jaws is running in coarse grained mode!")
            hiveContext.sparkContext.cancelJobGroup(message.queryID)
          } else {
            Configuration.log4j.info("[RunScriptApiActor]: Jaws is running in fine grained mode!")
          }

        }
      }

    }
  }

  private def splitPath(filePath: String): Tuple2[String, String] = {
    val matcher: Matcher = pattern.matcher(filePath)

    if (matcher.matches())
      (matcher.group(1), matcher.group(2))
    else
      throw new Exception(s"Invalid file path format : filePath")

  }
}