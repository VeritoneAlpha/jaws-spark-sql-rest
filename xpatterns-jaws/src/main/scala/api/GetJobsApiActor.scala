package api

import akka.actor.Actor
import akka.actor.actorRef2Scala
import messages.GetJobsMessage
import com.google.common.base.Preconditions
import actors.LogsActor
import akka.actor.ActorLogging
import traits.DAL
import model.Jobs
import model.Job
import actors.Configuration
/**
 * Created by emaorhian
 */
class GetJobsApiActor (dals: DAL) extends Actor{
  
  override def receive = {
    
  	case message : GetJobsMessage => {
      
		Configuration.log4j.info("[GetJobsApiActor]: retrieving " + message.limit + " number of jobs starting with " + message.startUuid)
		Preconditions.checkArgument(message.limit != null, Configuration.LIMIT_EXCEPTION_MESSAGE)
		val jobStates = dals.loggingDal.getStateOfJobs(message.startUuid, message.limit)
		val jobs = new Array[Job](jobStates.size())

		var index = 0
		val iterator = jobStates.iterator()
		while (iterator.hasNext()){
		  val jobState = iterator.next()
		  jobs(index) = new Job(jobState.state.name(), jobState.uuid, dals.loggingDal.getScriptDetails(jobState.uuid))
		  index = index + 1
		}
		
		val returnVal = new Jobs(jobs)
		Configuration.log4j.debug("[GetJobsApiActor]: Returning jobs")
		sender ! returnVal

    }
  }
}