package org.apache.spark.scheduler

import scala.collection.mutable.Map
import org.json4s.jackson.JsonMethods._
import server.Configuration
import server.LogsActor.PushLogs
import server.MainActors._
import traits.DAL
import org.apache.spark.util.JsonProtocol
import org.apache.spark.SparkContext
import org.apache.spark.Success
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.Resubmitted
import org.apache.spark.FetchFailed

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

  override def onJobEnd(jobEnd: SparkListenerJobEnd) = {
    val jobId = jobEnd.jobId
    jobIdToUUID.get(jobId) match {
      case Some(uuid) => {
        jobIdToStartTimestamp.get(jobId) match {
          case Some(startTimestamp) => {
            val executionTime = (System.currentTimeMillis() - startTimestamp) / 1000.0
            val executionTimeMessage = "The job " + jobId + " has finished in " + executionTime + " s ."

            var info = "JOB_ID=" + jobId
            jobEnd.jobResult match {
              case JobSucceeded => info += " STATUS=SUCCESS"
              case JobFailed(exception) =>
                info += " STATUS=FAILED REASON="
                exception.getMessage.split("\\s+").foreach(info += _ + "_")
              case _ =>
            }
            logInfo(uuid, jobId.toString, info)
            logInfo(uuid, jobId.toString, executionTimeMessage)

            jobIdToUUID.remove(jobId)
            jobIdToStartTimestamp.remove(jobId)

          }
          case None => Configuration.log4j.debug("[LoggingListener]- onJobEnd :  There is no such job id!")
        }

      }
      case None => Configuration.log4j.debug("[LoggingListener]- onJobEnd :  There is no such uuid!")
    }

  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val properties = jobStart.properties
    Option(properties) match {
      case None => Configuration.log4j.info("[LoggingListener - onJobStart] properties file is null")
      case _ => {
        val uuid = properties.getProperty("spark.jobGroup.id")
        val jobId = jobStart.jobId        
        jobStart.properties.setProperty(JOB_ID, jobId.toString())
        if (!uuid.isEmpty()) {
          jobIdToStartTimestamp.put(jobId, System.currentTimeMillis())
          jobIdToUUID.put(jobId, uuid)
          logInfo(uuid, jobId.toString, s"The job $jobId has started. Executing command.")
          logInfo(uuid, jobId.toString, properties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION, ""))
        }
      }
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    val stageId = stageSubmitted.stageInfo.stageId
    val properties = stageSubmitted.properties
    Option(properties) match {
      case None => Configuration.log4j.debug("[LoggingListener - onStageSubmitted] properties file is null")
      case _ => {
        val jobIdProperty = stageSubmitted.properties.getProperty(JOB_ID)
        Option(jobIdProperty) match {
          case None => Configuration.log4j.debug("[LoggingListener - onStageSubmitted] jobIdProperty file is null")
          case _ => {

            val jobId = Integer.parseInt(jobIdProperty)

            jobIdToUUID.get(jobId) match {
              case Some(uuid) => {
                stageIdToJobId.put(stageId, jobId)
                stageIdToNrTasks.put(stageId, stageSubmitted.stageInfo.numTasks)
                stageIdToSuccessullTasks.put(stageId, 0)

                logInfo(uuid, jobId.toString, s"The stage $stageId was submitted for job $jobId. TASK_SIZE=${stageSubmitted.stageInfo.numTasks.toString}")
              }
              case None => Configuration.log4j.debug("[LoggingListener]- onStageSubmitted :  There is no such jobId " + jobId + "!")
            }
          }
        }
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val taskId = taskEnd.taskInfo.taskId
    val stageId = taskEnd.stageId
    stageIdToJobId.get(stageId) match {
      case Some(jobId) => {

        jobIdToUUID.get(jobId) match {
          case Some(uuid) => {

            var taskStatus = s"TASK_TYPE=${taskEnd.taskType}"

            taskEnd.reason match {
              case Success =>
                taskStatus += " STATUS=SUCCESS"
                stageIdToSuccessullTasks.get(stageId) match {
                  case Some(successfulTasks) => stageIdToSuccessullTasks.put(stageId, successfulTasks + 1)
                  case None => stageIdToSuccessullTasks.put(stageId, 1)
                }
                logInfo(uuid, jobId.toString(), s"The task $taskId belonging to stage $stageId for job $jobId has finished in ${taskEnd.taskInfo.duration} ms on ${taskEnd.taskInfo.host} ( progress ${stageIdToSuccessullTasks.get(stageId).get}/${stageIdToNrTasks.get(stageId).get})")
              case Resubmitted =>
                taskStatus += s" STATUS=RESUBMITTED TID=${taskEnd.taskInfo.taskId} STAGE_ID=${taskEnd.stageId}"
              case FetchFailed(bmAddress, shuffleId, mapId, reduceId) =>
                taskStatus += s" STATUS=FETCHFAILED TID=${taskEnd.taskInfo.taskId} STAGE_ID=${taskEnd.stageId} SHUFFLE_ID=$shuffleId MAP_ID=$mapId REDUCE_ID=$reduceId"
                logInfo(uuid, jobId.toString(), s"The task $taskId belonging to stage $stageId for job $jobId has failed! Duration was ${taskEnd.taskInfo.duration} ms on ${taskEnd.taskInfo.host}")
              case _ =>
            }
            logInfo(uuid, jobId.toString(), taskStatus)
          }
          case None => Configuration.log4j.debug("[LoggingListener]- onTaskEnd :  There is no such jobId " + jobId + "!")
        }
      }

      case None => Configuration.log4j.debug("[LoggingListener]- onTaskEnd :  There is no such stageId " + stageId + "!")
    }

  }

  override def onTaskStart(taskStart: SparkListenerTaskStart) {
    val taskId = taskStart.taskInfo.taskId
    val stageId = taskStart.stageId
    stageIdToJobId.get(stageId) match {
      case Some(jobId) => {
        jobIdToUUID.get(jobId) match {
          case Some(uuid) => {
            logInfo(uuid, jobId.toString, s"The task $taskId belonging to stage $stageId for job $jobId has started on ${taskStart.taskInfo.host}")
          }
          case None => Configuration.log4j.debug("[LoggingListener]- onTaskStart :  There is no such jobId " + jobId + "!")
        }
      }
      case None => Configuration.log4j.debug("[LoggingListener]- onTaskStart :  There is no such stageId " + stageId + "!")
    }

  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {

    val stageId = stageCompleted.stageInfo.stageId
    stageIdToJobId.get(stageId) match {
      case Some(jobId) => {
        jobIdToUUID.get(jobId) match {
          case Some(uuid) => {
            val executionTime = (stageCompleted.stageInfo.completionTime.get - stageCompleted.stageInfo.submissionTime.get) / 1000.0
            stageIdToJobId.remove(stageId)
            stageIdToNrTasks.remove(stageId)
            stageIdToSuccessullTasks.remove(stageId)
            var status = "STATUS=COMPLETED"
            if (stageCompleted.stageInfo.failureReason.isEmpty == false) {
              status = "STATUS=FAILED"
            }
            logInfo(uuid, jobId.toString, s"The stage $stageId for job $jobId has finished in $executionTime s with $status!")

          }
          case None => Configuration.log4j.debug("[LoggingListener]- onStageCompleted :  There is no such jobId " + jobId + "!")
        }
      }
      case None => Configuration.log4j.debug("[LoggingListener]- onStageCompleted :  There is no such stageId " + stageId + "!")
    }

  }

  def logInfo(uuid: String, jobId: String, message: String) = {
    Configuration.log4j.info(message)
    HiveUtils.logMessage(uuid, message, jobId, dals.loggingDal)
  }
}
