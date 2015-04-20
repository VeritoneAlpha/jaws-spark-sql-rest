package apiactors

import akka.actor.Actor
import messages.GetQueriesMessage
import com.google.common.base.Preconditions
import com.xpatterns.jaws.data.contracts.DAL
import com.xpatterns.jaws.data.DTO.Queries
import com.xpatterns.jaws.data.DTO.Query
import server.Configuration
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage
/**
 * Created by emaorhian
 */
class GetQueriesApiActor(dals: DAL) extends Actor {

  override def receive = {

    case message: GetQueriesMessage => {

      Configuration.log4j.info("[GetQueriesApiActor]: retrieving " + message.limit + " number of queries starting with " + message.startQueryID)
      val currentSender = sender
      val getQueriesFuture = future {
        dals.loggingDal.getQueriesStates(message.startQueryID, message.limit)
      }

      getQueriesFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET queries failed with the following message: ${e.getMessage}")
      }
    }
  }
}