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
import org.apache.spark.scheduler.RunSharkScriptTask
import implementation.HiveContextWrapper

/**
 * Created by emaorhian
 */
class RunScriptApiActor(hdfsConf: org.apache.hadoop.conf.Configuration, hiveContext: HiveContextWrapper, dals: DAL) extends Actor {
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
      val task = new RunSharkScriptTask(dals, message.hqlScript, hiveContext, uuid, false, message.limited, message.maxNumberOfResults, hdfsConf, message.rddDestination)
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
}