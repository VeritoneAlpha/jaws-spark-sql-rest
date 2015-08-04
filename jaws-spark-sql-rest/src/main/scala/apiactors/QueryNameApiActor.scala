package apiactors

import akka.actor.Actor
import com.xpatterns.jaws.data.contracts.DAL
import com.xpatterns.jaws.data.utils.QueryState
import messages.{UpdateQueryNameMessage, ErrorMessage}
import server.Configuration
import scala.concurrent._
import ExecutionContext.Implicits.global

import scala.concurrent._
import scala.util.{Failure, Success}

/**
 * Handles the name operation on a query
 */
class QueryNameApiActor (dals: DAL) extends Actor {
  override def receive = {
    case message: UpdateQueryNameMessage =>

      Configuration.log4j.info(s"[NameQueryApiActor]: updating query id ${message.queryID} with name ${message.name}")

      val currentSender = sender()

      val updateQueryFuture = future {
        dals.loggingDal.getState(message.queryID) match {
          case QueryState.NOT_FOUND => throw new Exception(s"The query ${message.queryID} was not found. Please provide a valid query id")
          case _ =>
            dals.loggingDal.setQueryName(message.queryID, message.name, message.description, message.overwrite)
            s"Query information for ${message.queryID} has been updated"
        }
      }

      updateQueryFuture onComplete {
        case Success(successfulMessage) => currentSender ! successfulMessage
        case Failure(e) => currentSender ! ErrorMessage(s"Updating query failed with the following message: ${e.getMessage}")
      }
  }
}
