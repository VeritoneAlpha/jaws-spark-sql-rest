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
      
		Configuration.log4j.info("[GetQueriesApiActor]: retrieving " + message.limit + " number of jobs starting with " + message.startQueryID)
		Preconditions.checkArgument(message.limit != null, Configuration.LIMIT_EXCEPTION_MESSAGE)
		val jobStates = dals.loggingDal.getStateOfJobs(message.startQueryID, message.limit)
		val jobs = new Array[Query](jobStates.size())

		var index = 0
		val iterator = jobStates.iterator()
		while (iterator.hasNext()){
		  val jobState = iterator.next()
		  jobs(index) = new Query(jobState.state.name(), jobState.uuid, dals.loggingDal.getScriptDetails(jobState.uuid))
		  index = index + 1
		}
		
		val returnVal = new Queries(jobs)
		Configuration.log4j.debug("[GetQueriesApiActor]: Returning jobs")
		sender ! returnVal

    }
  }
}