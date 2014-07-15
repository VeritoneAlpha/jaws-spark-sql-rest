package api

import akka.actor.Actor
import akka.actor.actorRef2Scala
import messages.GetQueriesMessage
import com.google.common.base.Preconditions
import actors.LogsActor
import akka.actor.ActorLogging
import traits.DAL
import model.Queries
import model.Query
import actors.Configuration
/**
 * Created by emaorhian
 */
class GetQueriesApiActor (dals: DAL) extends Actor{
  
  override def receive = {
    
  	case message : GetQueriesMessage => {
      
		Configuration.log4j.info("[GetQueriesApiActor]: retrieving " + message.limit + " number of queries starting with " + message.startQueryID)
		Preconditions.checkArgument(message.limit != null, Configuration.LIMIT_EXCEPTION_MESSAGE)
		val queriesStates = dals.loggingDal.getQueriesStates(message.startQueryID, message.limit)
		val queries = new Array[Query](queriesStates.size())

		var index = 0
		val iterator = queriesStates.iterator()
		while (iterator.hasNext()){
		  val queryState = iterator.next()
		  queries(index) = new Query(queryState.state.name(), queryState.uuid, dals.loggingDal.getScriptDetails(queryState.uuid))
		  index = index + 1
		}
		
		val returnVal = new Queries(queries)
		Configuration.log4j.debug("[GetQueriesApiActor]: Returning queries")
		sender ! returnVal

    }
  }
}