package apiactors

import akka.actor.Actor
import com.google.common.base.Preconditions
import com.xpatterns.jaws.data.contracts.DAL
import com.xpatterns.jaws.data.DTO.Queries
import com.xpatterns.jaws.data.DTO.Query
import server.Configuration
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage
import messages.GetPaginatedQueriesMessage
import messages.GetQueriesMessage
/**
 * Created by emaorhian
 */
class GetQueriesApiActor(dals: DAL) extends Actor {

  override def receive = {

    case message: GetPaginatedQueriesMessage => {

      Configuration.log4j.info("[GetQueriesApiActor]: retrieving " + message.limit + " number of queries starting with " + message.startQueryID)
      val currentSender = sender
      val getQueriesFuture = future {
        dals.loggingDal.getQueries(message.startQueryID, message.limit)
      }

      getQueriesFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET queries failed with the following message: ${e.getMessage}")
      }
    }

    case message: GetQueriesMessage => {
      Configuration.log4j.info("[GetQueryInfoApiActor]: retrieving the query information for " + message.queryIDs)

      val currentSender = sender

      val getQueryInfoFuture = future {
        dals.loggingDal.getQueries(message.queryIDs)

      }

      getQueryInfoFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET query info failed with the following message: ${e.getMessage}")
      }

    }
  }

}