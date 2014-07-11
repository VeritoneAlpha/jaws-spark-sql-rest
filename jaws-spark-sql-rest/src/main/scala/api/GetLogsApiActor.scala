package api

import akka.actor.Actor
import akka.actor.actorRef2Scala
import messages.GetJobsMessage
import com.google.common.base.Preconditions
import actors.LogsActor
import akka.actor.ActorLogging
import traits.DAL
import model.Queries
import model.Query
import messages.GetLogsMessage
import org.joda.time.DateTime
import java.util.Collection
import model.Logs
import model.Log
import actors.Configuration
/**
 * Created by emaorhian
 */
class GetLogsApiActor(dals: DAL) extends Actor {

  override def receive = {

    case message: GetLogsMessage => {
      Configuration.log4j.info("[GetLogsApiActor]: retrieving logs for: " + message.uuid)
      Preconditions.checkArgument(message.uuid != null && !message.uuid.isEmpty(), Configuration.UUID_EXCEPTION_MESSAGE)
      var startDate = message.startDate
      var limit = message.limit

      Option(message.startDate) match {
        case None => {
          startDate = new DateTime(1977, 1, 1, 1, 1, 1, 1).getMillis()
        }
        case _ => Configuration.log4j.debug("[GetLogsApiActor]: Start date = " + startDate)
        
      }

      Option(limit) match {
        case None => limit = 100
        case _ => Configuration.log4j.debug("[GetLogsApiActor]: Limit = " + limit)
      }
      
      // retrieving the status earlier because otherwise we might lose the
      // last logs
      val status = dals.loggingDal.getState(message.uuid).name()
      val logs = dals.loggingDal.getLogs(message.uuid, startDate, limit)

      sender ! Logs(Log.getLogArray(logs), status)

    }
  }
}