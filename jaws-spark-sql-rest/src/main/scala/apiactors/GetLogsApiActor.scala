package apiactors

import akka.actor.Actor
import akka.actor.actorRef2Scala
import apiactors.ActorOperations._
import messages.GetQueriesMessage
import com.google.common.base.Preconditions
import server.LogsActor
import akka.actor.ActorLogging
import com.xpatterns.jaws.data.contracts.DAL
import messages.GetLogsMessage
import org.joda.time.DateTime
import java.util.Collection
import server.Configuration
import com.xpatterns.jaws.data.DTO.Logs
import com.xpatterns.jaws.data.DTO.Log

import scala.util.Try

/**
 * Created by emaorhian
 */
class GetLogsApiActor(dals: DAL) extends Actor {

  override def receive = {

    case message: GetLogsMessage => {
      Configuration.log4j.info("[GetLogsApiActor]: retrieving logs for: " + message.queryID)
      var logs : Logs = null
      val tryGetLogs = Try {
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
      logs = dals.loggingDal.getLogs(message.queryID, startDate, limit)
      }
      returnResult(tryGetLogs, logs, "GET logs failed with the following message: ", sender)
    }
  }
}