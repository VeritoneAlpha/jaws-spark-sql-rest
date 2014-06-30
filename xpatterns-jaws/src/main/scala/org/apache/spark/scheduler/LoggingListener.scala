package org.apache.spark.scheduler

import scala.collection.mutable.Map

import actors.Configuration
import actors.LogsActor.PushLogs
import actors.MainActors
import actors.Systems
import akka.actor.ActorRef
import traits.DAL

/**
 * Created by emaorhian
 */
class LoggingListener(dals: DAL) extends SparkListener with MainActors with Systems {

  val JOB_ID: String = "xpatterns.job.id"
  val commentSize = 2
  val jobIdToUUID = Map[Integer, String]()
  val jobIdToStartTimestamp = Map[Integer, Long]()
  val stageIdToJobId = Map[Integer, Integer]()
  val stageIdToNrTasks = Map[Integer, Integer]()
  val stageIdToSuccessullTasks = Map[Integer, Integer]()

  override def onJobEnd(arg0: SparkListenerJobEnd) = {
    val jobId = arg0.job.jobId
    jobIdToUUID.get(jobId) match {
      case Some(uuid) => {
        jobIdToStartTimestamp.get(jobId) match {
          case Some(startTimestamp) => {
            val executionTime = (System.currentTimeMillis() - startTimestamp) / 1000.0
            val message = "The job " + jobId + " has finished in " + executionTime + " s ."

            jobIdToUUID.remove(jobId)
            jobIdToStartTimestamp.remove(jobId)
            try {
              dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), message)
              logsActor ! new PushLogs(uuid, message)

            } catch {
              case e: Exception => Configuration.log4j.error(e)
            }
            Configuration.log4j.info(message)

          }
          case None => Configuration.log4j.warn("[LoggingListener]- onJobEnd :  There is no such job id!")
        }

      }
      case None => Configuration.log4j.warn("[LoggingListener]- onJobEnd :  There is no such uuid!")
    }

  }

  override def onJobStart(arg0: SparkListenerJobStart) {
    val properties = arg0.job.properties
    Option(properties) match {
      case None => Configuration.log4j.info("[LoggingListener - onJobStart] properties file is null")
      case _ => {
        val command = properties.getProperty("spark.job.description")
        val jobId = arg0.job.jobId
        arg0.properties.setProperty(JOB_ID, jobId.toString())

        // extract the uuid from comment
        val index = command.indexOf("\n")
        if (index >= 0) {
          val uuid = command.substring(commentSize, index)
          val message = "The job " + jobId + " has started. Executing command " + command

          jobIdToStartTimestamp.put(jobId, System.currentTimeMillis())
          jobIdToUUID.put(jobId, uuid)
          try {
            dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), message)
            logsActor ! new PushLogs(uuid, message)
          } catch {
            case e: Exception => Configuration.log4j.error(e)
          }
        }
      }
    }
  }

  override def onStageSubmitted(arg0: SparkListenerStageSubmitted) {
    val stageId = arg0.stage.stageId
    val properties = arg0.properties
    Option(properties) match {
      case None => Configuration.log4j.info("[LoggingListener - onStageSubmitted] properties file is null")
      case _ => {

        val jobIdProperty = arg0.properties.getProperty(JOB_ID)
        Option(jobIdProperty) match {
          case None => Configuration.log4j.info("[LoggingListener - onStageSubmitted] jobIdProperty file is null")
          case _ => {

            val jobId = Integer.parseInt(jobIdProperty)

            jobIdToUUID.get(jobId) match {
              case Some(uuid) => {
                val message = "The stage " + stageId + " was submitted for job " + jobId

                stageIdToJobId.put(stageId, jobId)
                stageIdToNrTasks.put(stageId, arg0.stage.numTasks)
                stageIdToSuccessullTasks.put(stageId, 0)

                try {
                  dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), message)
                  logsActor ! new PushLogs(uuid, message)
                } catch {
                  case e: Exception => Configuration.log4j.error(e)
                }
                Configuration.log4j.info(message)
              }
              case None => Configuration.log4j.warn("[LoggingListener]- onStageSubmitted :  There is no such jobId " + jobId + "!")
            }
          }
        }
      }
    }
  }

  override def onTaskEnd(arg0: SparkListenerTaskEnd) {
    val taskId = arg0.taskInfo.taskId
    val stageId = arg0.task.stageId
    stageIdToJobId.get(stageId) match {
      case Some(jobId) => {

        jobIdToUUID.get(jobId) match {
          case Some(uuid) => {
            var message = ""
            if (arg0.taskInfo.successful) {
              stageIdToSuccessullTasks.get(stageId) match {
                case Some(successfulTasks) => stageIdToSuccessullTasks.put(stageId, successfulTasks + 1)
                case None => stageIdToSuccessullTasks.put(stageId, 1)

              }
              message = "The task " + taskId + " belonging to stage " + stageId + " for job " + jobId + " has finished in " + arg0.taskInfo.duration + " ms on " + arg0.taskInfo.host + "( progress " + stageIdToSuccessullTasks.get(stageId) + "/" + stageIdToNrTasks.get(stageId) + " )"

            } else {
              message = "The task " + taskId + " belonging to stage " + stageId + " for job " + jobId + " has failed! Duration was " + arg0.taskInfo.duration + " ms on " + arg0.taskInfo.host
            }

            try {
              dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), message)
              logsActor.tell(new PushLogs(uuid, message), null)
            } catch {
              case e: Exception => Configuration.log4j.error(e)
            }
            Configuration.log4j.info(message)
          }
          case None => Configuration.log4j.warn("[LoggingListener]- onTaskEnd :  There is no such jobId " + jobId + "!")
        }
      }

      case None => Configuration.log4j.warn("[LoggingListener]- onTaskEnd :  There is no such stageId " + stageId + "!")
    }

  }

  override def onTaskStart(arg0: SparkListenerTaskStart) {
    val taskId = arg0.taskInfo.taskId
    val stageId = arg0.task.stageId
    stageIdToJobId.get(stageId) match {
      case Some(jobId) => {
        jobIdToUUID.get(jobId) match {
          case Some(uuid) => {
            val message = "The task " + taskId + " belonging to stage " + stageId + " for job " + jobId + " has started on " + arg0.taskInfo.host

            try {
              dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), message)
              logsActor.tell(new PushLogs(uuid, message), null)
            } catch {
              case e: Exception => Configuration.log4j.error(e)
            }
            Configuration.log4j.info(message)
          }
          case None => Configuration.log4j.warn("[LoggingListener]- onTaskStart :  There is no such jobId " + jobId + "!")
        }

      }
      case None => Configuration.log4j.warn("[LoggingListener]- onTaskStart :  There is no such stageId " + stageId + "!")
    }

  }

  override def onTaskGettingResult(arg0: SparkListenerTaskGettingResult) {

  }

  override def onStageCompleted(arg0: SparkListenerStageCompleted) {

    val stageId = arg0.stage.stageId

    stageIdToJobId.get(stageId) match {
      case Some(jobId) => {
        jobIdToUUID.get(jobId) match {
          case Some(uuid) => {
            val executionTime = (arg0.stage.completionTime.get - arg0.stage.submissionTime.get) / 1000.0

            val message = "The stage " + stageId + " for job " + jobId + " has finished in " + executionTime + " s !"

            stageIdToJobId.remove(stageId)
            stageIdToNrTasks.remove(stageId)
            stageIdToSuccessullTasks.remove(stageId)
            try {
              dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), message)
              logsActor ! new PushLogs(uuid, message)
            } catch {
              case e: Exception => Configuration.log4j.error(e)
            }
            Configuration.log4j.info(message)
          }
          case None => Configuration.log4j.warn("[LoggingListener]- onStageCompleted :  There is no such jobId " + jobId + "!")
        }
      }
      case None => Configuration.log4j.warn("[LoggingListener]- onStageCompleted :  There is no such stageId " + stageId + "!")
    }

  }

}
