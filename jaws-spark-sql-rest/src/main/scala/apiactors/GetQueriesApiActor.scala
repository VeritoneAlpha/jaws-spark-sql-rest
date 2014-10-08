package apiactors

import akka.actor.Actor
import akka.actor.actorRef2Scala
import messages.GetQueriesMessage
import com.google.common.base.Preconditions
import server.LogsActor
import akka.actor.ActorLogging
import traits.DAL
import com.xpatterns.jaws.data.DTO.Queries
import com.xpatterns.jaws.data.DTO.Query
import server.Configuration
/**
 * Created by emaorhian
 */
class GetQueriesApiActor (dals: DAL) extends Actor{
  
  override def receive = {
    
  	case message : GetQueriesMessage => {
      
		Configuration.log4j.info("[GetQueriesApiActor]: retrieving " + message.limit + " number of queries starting with " + message.startQueryID)
		Preconditions.checkArgument(message.limit != null, Configuration.LIMIT_EXCEPTION_MESSAGE)
		val queriesStates = dals.loggingDal.getQueriesStates(message.startQueryID, message.limit)
		Configuration.log4j.debug("[GetQueriesApiActor]: Returning queries")
		sender ! queriesStates

    }
  }
}