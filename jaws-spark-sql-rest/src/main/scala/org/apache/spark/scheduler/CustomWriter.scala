package org.apache.spark.scheduler

import java.io.PrintWriter
import java.io.Writer
import actors.Configuration
import actors.LogsActor.PushLogs
import actors.MainActors
import actors.Systems
import akka.actor.actorRef2Scala
import traits.DAL

/**
 * Created by emaorhian
 */
class CustomWriter(jobId: Integer, w: Writer, dals: DAL) extends PrintWriter(w) with MainActors with Systems {
  override def print(message: String) = {

    CustomJobLogger.jobIdToUUID.get(jobId) match {
      case Some(uuid) => {
        try {
          dals.loggingDal.addLog(uuid, jobId.toString(), System.currentTimeMillis(), message)
          logsActor ! new PushLogs(uuid, message)
        } catch {
          case e: Exception => Configuration.log4j.error(e)
        }
        Configuration.log4j.info(message)
      }
      case None => Configuration.log4j.error("[CustomWriter] No such uuid")
    }
  }

}