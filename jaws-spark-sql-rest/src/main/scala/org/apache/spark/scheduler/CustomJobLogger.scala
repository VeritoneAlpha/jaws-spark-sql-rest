package org.apache.spark.scheduler

import com.xpatterns.jaws.data.contracts.TJawsLogging
import akka.actor.ActorRef
import actors.Systems
import actors.MainActors
import traits.CustomSharkContext
import traits.DAL
import java.io.StringWriter
import scala.collection.mutable.Map
import actors.Configuration

/**
 * Created by emaorhian
 */
class CustomJobLogger(dals: DAL) extends JobLogger with MainActors with Systems {

  val UUID_PROPERTY_NAME: String = "spark.job.description"
  val commentSize = 2

  override def createLogWriter(jobID: Int) {
    getJobIDtoPrintWriter.put(jobID, new CustomWriter(jobID, new StringWriter(), dals))

  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val properties = jobStart.properties
    Option(properties) match {
      case None => Configuration.log4j.info("[CustomJobLogger - onJobStart] properties file is null")
      case _ => {
        val command = properties.getProperty(UUID_PROPERTY_NAME)
        val index = command.indexOf("\n")
        if (index >= 0) {
          val uuid = command.substring(commentSize, index)
          CustomJobLogger.jobIdToUUID.put(jobStart.job.jobId, uuid)
        }

        super.onJobStart(jobStart)
      }
    }

  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val jobId = jobEnd.job.jobId
    val uuid = CustomJobLogger.jobIdToUUID.get(jobId)
    Option(uuid) match {
      case None => Configuration.log4j.info("[CustomJobLogger - onJobEnd] uuid is null")
      case _ => {
        CustomJobLogger.jobIdToUUID.remove(jobId)
      }
    }

    super.onJobEnd(jobEnd)
  }
}

object CustomJobLogger {
  val jobIdToUUID = scala.collection.mutable.Map[Integer, String]()
}
