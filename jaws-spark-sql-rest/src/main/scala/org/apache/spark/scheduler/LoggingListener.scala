package org.apache.spark.scheduler

import scala.collection.mutable.Map
import org.json4s.jackson.JsonMethods._
import server.Configuration
import server.LogsActor.PushLogs
import server.MainActors._
import traits.DAL
import org.apache.spark.util.JsonProtocol

/**
 * Created by emaorhian
 */
class LoggingListener(dals: DAL) extends SparkListener {

  val JOB_ID: String = "xpatterns.job.id"
  val commentSize = 2
  val jobIdToUUID = Map[Integer, String]()
  val jobIdToStartTimestamp = Map[Integer, Long]()
  val stageIdToJobId = Map[Integer, Integer]()
  val stageIdToNrTasks = Map[Integer, Integer]()
  val stageIdToSuccessullTasks = Map[Integer, Integer]()

  override def onJobEnd(arg0: SparkListenerJobEnd) = {
    val jobId = arg0.jobId
    
    val json = JsonProtocol.sparkEventToJson(arg0)
    val compactedEvent = compact(render(json))
    
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
              dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), compactedEvent)
              logsActor ! new PushLogs(uuid, compactedEvent)
              
            } catch {
              case e: Exception => Configuration.log4j.error(e)
            }
            Configuration.log4j.info(message)

          }
          case None => Configuration.log4j.debug("[LoggingListener]- onJobEnd :  There is no such job id!")
        }

      }
      case None => Configuration.log4j.debug("[LoggingListener]- onJobEnd :  There is no such uuid!")
    }

  }

  
  override def onJobStart(arg0: SparkListenerJobStart) {
    val properties = arg0.properties
    
    val json = JsonProtocol.sparkEventToJson(arg0)
    val compactedEvent = compact(render(json))
    
    Option(properties) match {
      case None => Configuration.log4j.info("[LoggingListener - onJobStart] properties file is null")
      case _ => {
        val uuid = properties.getProperty("spark.jobGroup.id")
        val jobId = arg0.jobId
        arg0.properties.setProperty(JOB_ID, jobId.toString())
      
        if (!uuid.isEmpty()) {
          
          val message = "The job " + jobId + " has started. Executing command."

          jobIdToStartTimestamp.put(jobId, System.currentTimeMillis())
          jobIdToUUID.put(jobId, uuid)
          try {
            dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), message)
            logsActor ! new PushLogs(uuid, message)
            dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), compactedEvent)
            logsActor ! new PushLogs(uuid, compactedEvent)
            
          } catch {
            case e: Exception => Configuration.log4j.error(e)
          }
        }
      }
    }
  }

  override def onStageSubmitted(arg0: SparkListenerStageSubmitted) {

    val json = JsonProtocol.sparkEventToJson(arg0)
    val compactedEvent = compact(render(json))

    val stageId = arg0.stageInfo.stageId
    val properties = arg0.properties
    Option(properties) match {
      case None => Configuration.log4j.debug("[LoggingListener - onStageSubmitted] properties file is null")
      case _ => {

        val jobIdProperty = arg0.properties.getProperty(JOB_ID)
        Option(jobIdProperty) match {
          case None => Configuration.log4j.debug("[LoggingListener - onStageSubmitted] jobIdProperty file is null")
          case _ => {

            val jobId = Integer.parseInt(jobIdProperty)

            jobIdToUUID.get(jobId) match {
              case Some(uuid) => {
                val message = "The stage " + stageId + " was submitted for job " + jobId

                stageIdToJobId.put(stageId, jobId)
                stageIdToNrTasks.put(stageId, arg0.stageInfo.numTasks)
                stageIdToSuccessullTasks.put(stageId, 0)

                try {
                  dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), message)
                  logsActor ! new PushLogs(uuid, message)
                  dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), compactedEvent)
                  logsActor ! new PushLogs(uuid, compactedEvent)

                } catch {
                  case e: Exception => Configuration.log4j.error(e)
                }
                Configuration.log4j.info(message)
              }
              case None => Configuration.log4j.debug("[LoggingListener]- onStageSubmitted :  There is no such jobId " + jobId + "!")
            }
          }
        }
      }
    }
  }

  override def onTaskEnd(arg0: SparkListenerTaskEnd) {
    val taskId = arg0.taskInfo.taskId
    val stageId = arg0.stageId
    val json = JsonProtocol.sparkEventToJson(arg0)
    val compactedEvent = compact(render(json))

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
              logsActor ! new PushLogs(uuid, message)
              dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), compactedEvent)
              logsActor ! new PushLogs(uuid, compactedEvent)
            } catch {
              case e: Exception => Configuration.log4j.error(e)
            }
            Configuration.log4j.info(message)
          }
          case None => Configuration.log4j.debug("[LoggingListener]- onTaskEnd :  There is no such jobId " + jobId + "!")
        }
      }

      case None => Configuration.log4j.debug("[LoggingListener]- onTaskEnd :  There is no such stageId " + stageId + "!")
    }

  }

  override def onTaskStart(arg0: SparkListenerTaskStart) {
    val taskId = arg0.taskInfo.taskId
    val stageId = arg0.stageId
    val json = JsonProtocol.sparkEventToJson(arg0)
    val compactedEvent = compact(render(json))

    stageIdToJobId.get(stageId) match {
      case Some(jobId) => {
        jobIdToUUID.get(jobId) match {
          case Some(uuid) => {
            val message = "The task " + taskId + " belonging to stage " + stageId + " for job " + jobId + " has started on " + arg0.taskInfo.host

            try {
              dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), message)
              logsActor ! new PushLogs(uuid, message)
              dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), compactedEvent)
              logsActor ! new PushLogs(uuid, compactedEvent)

            } catch {
              case e: Exception => Configuration.log4j.error(e)
            }
            Configuration.log4j.info(message)
          }
          case None => Configuration.log4j.debug("[LoggingListener]- onTaskStart :  There is no such jobId " + jobId + "!")
        }

      }
      case None => Configuration.log4j.debug("[LoggingListener]- onTaskStart :  There is no such stageId " + stageId + "!")
    }

  }

  override def onStageCompleted(arg0: SparkListenerStageCompleted) {

    val stageId = arg0.stageInfo.stageId
    val json = JsonProtocol.sparkEventToJson(arg0)
    val compactedEvent = compact(render(json))

    stageIdToJobId.get(stageId) match {
      case Some(jobId) => {
        jobIdToUUID.get(jobId) match {
          case Some(uuid) => {
            val executionTime = (arg0.stageInfo.completionTime.get - arg0.stageInfo.submissionTime.get) / 1000.0

            val message = "The stage " + stageId + " for job " + jobId + " has finished in " + executionTime + " s !"

            stageIdToJobId.remove(stageId)
            stageIdToNrTasks.remove(stageId)
            stageIdToSuccessullTasks.remove(stageId)
            try {
              dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), message)
              logsActor ! new PushLogs(uuid, message)
              dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), compactedEvent)
              logsActor ! new PushLogs(uuid, compactedEvent)
            } catch {
              case e: Exception => Configuration.log4j.error(e)
            }
            Configuration.log4j.info(message)
          }
          case None => Configuration.log4j.debug("[LoggingListener]- onStageCompleted :  There is no such jobId " + jobId + "!")
        }
      }
      case None => Configuration.log4j.debug("[LoggingListener]- onStageCompleted :  There is no such stageId " + stageId + "!")
    }

  }

}
