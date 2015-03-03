package apiactors

import traits.DAL
import akka.actor.Actor
import messages.DeleteQueryMessage
import server.Configuration
import scala.util.Try
import com.xpatterns.jaws.data.utils.QueryState
import apiactors.ActorOperations._

class DeleteQueryApiActor(dals: DAL) extends Actor {
  override def receive = {

    case message: DeleteQueryMessage => {

      Configuration.log4j.info(s"[DeleteQueryApiActor]: deleting query with id ${message.queryID}")

      val tryDeleteQuery = Try {
        dals.loggingDal.getState(message.queryID) match {
          case QueryState.IN_PROGRESS => throw new Exception(s"The query ${message.queryID} is IN_PROGRESS. Please wait for its completion or cancel it")
          case QueryState.NOT_FOUND => throw new Exception(s"The query ${message.queryID} was not found. Please provide a valid query id")          
          case _ => {
            dals.loggingDal.deleteQuery(message.queryID)
            dals.resultsDal.deleteResults(message.queryID)
          }
        }

      }

      returnResult(tryDeleteQuery, s"Query ${message.queryID} was deleted", "DELETE query failed with the following message: ", sender)

    }
  }
}