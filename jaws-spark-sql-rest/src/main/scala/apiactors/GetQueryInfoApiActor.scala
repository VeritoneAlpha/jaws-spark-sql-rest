package apiactors

import messages.GetQueryInfoMessage
import traits.DAL
import server.Configuration
import akka.actor.Actor
import com.google.common.base.Preconditions
import com.xpatterns.jaws.data.DTO.Query
import org.joda.convert.ToString
/**
 * Created by emaorhian
 */
class GetQueryInfoApiActor (dals: DAL) extends Actor{
  
  override def receive = {
    
  	case message : GetQueryInfoMessage => {
      Configuration.log4j.info("[GetQueryInfoApiActor]: retrieving the query information for " + message.queryID)
		Preconditions.checkArgument(message.queryID != null && !message.queryID.isEmpty(), Configuration.UUID_EXCEPTION_MESSAGE)
		
		sender ! new Query(dals.loggingDal.getState(message.queryID).toString, message.queryID, dals.loggingDal.getScriptDetails(message.queryID))
    }
  }
}
