package api

import akka.actor.Actor
import akka.actor.actorRef2Scala
import messages.GetQueriesMessage
import com.google.common.base.Preconditions
import actors.LogsActor
import akka.actor.ActorLogging
import traits.DAL
import com.xpatterns.jaws.data.DTO.Queries
import com.xpatterns.jaws.data.DTO.Query
import messages.RunScriptMessage
import java.util.UUID
import traits.CustomSharkContext
import actors.Configuration
import akka.actor.ActorRef
import com.google.common.cache.LoadingCache
import com.google.common.cache.CacheBuilder
import java.util.concurrent.TimeUnit
import com.google.common.cache.CacheLoader
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import actors.MainActors
import actors.Systems
import scala.collection.immutable.Nil
import com.google.common.cache.Cache
import shark.api.JavaSharkContext
import messages.CancelMessage
import org.apache.spark.scheduler.RunSharkScriptTask
import shapeless.ToInt
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by emaorhian
 */
class RunScriptApiActor(hdfsConf: org.apache.hadoop.conf.Configuration, hiveContext: HiveContext, dals: DAL) extends Actor with MainActors with Systems {
  var taskCache: Cache[String, RunSharkScriptTask] = _
  var threadPool: ThreadPoolTaskExecutor = _

  override def preStart() {
    taskCache = {
      CacheBuilder
        .newBuilder()
        .maximumSize(1000)
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build[String, RunSharkScriptTask]
    }

    threadPool = new ThreadPoolTaskExecutor()
    threadPool.setCorePoolSize(Configuration.nrOfThreads.getOrElse("10").toInt)
    threadPool.initialize()
  }

  override def receive = {

    case message: RunScriptMessage => {

      Configuration.log4j.info("[RunScriptApiActor]: running the following hql: " + message.hqlScript)
      Configuration.log4j.info("[RunScriptApiActor]: The script will be executed with the limited flag set on " + message.limited + ". The maximum number of results is " + message.maxNumberOfResults)
      Preconditions.checkArgument(message.hqlScript != null && !message.hqlScript.isEmpty(), Configuration.HQL_SCRIPT_EXCEPTION_MESSAGE)
      Preconditions.checkArgument(message.limited != null, Configuration.LIMITED_EXCEPTION_MESSAGE)
      Preconditions.checkArgument(message.maxNumberOfResults != null, Configuration.RESULSTS_NUMBER_EXCEPTION_MESSAGE)

      val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
      val task = new RunSharkScriptTask(dals, message.hqlScript, hiveContext, uuid, false, message.limited, message.maxNumberOfResults, hdfsConf)
      taskCache.put(uuid, task)
      threadPool.execute(task)

      sender ! uuid
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
          if (System.getProperty("spark.mesos.coarse").equalsIgnoreCase("true")) {
            Configuration.log4j.info("[RunScriptApiActor]: Jaws is running in coarse grained mode!")
            hiveContext.sparkContext.cancelJobGroup(message.queryID)
          } else {
            Configuration.log4j.info("[RunScriptApiActor]: Jaws is running in fine grained mode!")
          }

        }
      }

    }
  }
}