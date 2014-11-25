package apiactors

import akka.actor.Actor
import messages.GetQueriesMessage
import com.google.common.base.Preconditions
import traits.DAL
import com.xpatterns.jaws.data.DTO.Queries
import com.xpatterns.jaws.data.DTO.Query
import server.Configuration
import apiactors.ActorOperations._

import scala.util.Try

/**
 * Created by emaorhian
 */
class GetQueriesApiActor(dals: DAL) extends Actor {

  override def receive = {

    case message: GetQueriesMessage => {

      Configuration.log4j.info("[GetQueriesApiActor]: retrieving " + message.limit + " number of queries starting with " + message.startQueryID)
      var queriesStates = Queries(Array[Query]())

      val tryGetQueries = Try{
        Preconditions.checkArgument(message.limit != null, Configuration.LIMIT_EXCEPTION_MESSAGE)
        queriesStates = dals.loggingDal.getQueriesStates(message.startQueryID, message.limit)
      }
      Configuration.log4j.debug("[GetQueriesApiActor]: Returning queries")
      returnResult(tryGetQueries, queriesStates, "GET queries failed with the following message: ", sender)

    }
  }
}