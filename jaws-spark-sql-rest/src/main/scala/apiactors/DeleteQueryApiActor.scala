package apiactors

import com.xpatterns.jaws.data.contracts.DAL
import akka.actor.Actor
import messages.DeleteQueryMessage
import server.Configuration
import com.xpatterns.jaws.data.utils.QueryState
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage

class DeleteQueryApiActor(dals: DAL) extends Actor {
  override def receive = {

    case message: DeleteQueryMessage => {

      Configuration.log4j.info(s"[DeleteQueryApiActor]: deleting query with id ${message.queryID}")

      val currentSender = sender

      val deleteQueryFuture = future {
        dals.loggingDal.getState(message.queryID) match {
          case QueryState.IN_PROGRESS => throw new Exception(s"The query ${message.queryID} is IN_PROGRESS. Please wait for its completion or cancel it")
          case QueryState.NOT_FOUND => throw new Exception(s"The query ${message.queryID} was not found. Please provide a valid query id")
          case _ => {
            dals.loggingDal.deleteQuery(message.queryID)
            dals.resultsDal.deleteResults(message.queryID)
            s"Query ${message.queryID} was deleted"
          }
        }
      }

      deleteQueryFuture onComplete {
        case Success(successfulMessage) => currentSender ! successfulMessage
        case Failure(e) => currentSender ! ErrorMessage(s"DELETE query failed with the following message: ${e.getMessage}")
      }
    }
  }
}