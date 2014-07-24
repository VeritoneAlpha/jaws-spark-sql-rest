package api

import messages.GetQueryInfoMessage
import traits.DAL
import actors.Configuration
import akka.actor.Actor
import com.google.common.base.Preconditions
import com.xpatterns.jaws.data.DTO.QueryInfo
/**
 * Created by emaorhian
 */
class GetQueryInfoApiActor (dals: DAL) extends Actor{
  
  override def receive = {
    
  	case message : GetQueryInfoMessage => {
      Configuration.log4j.info("[GetQueryInfoApiActor]: retrieving the query information for " + message.queryID)
		Preconditions.checkArgument(message.queryID != null && !message.queryID.isEmpty(), Configuration.UUID_EXCEPTION_MESSAGE)
		sender ! new QueryInfo(dals.loggingDal.getState(message.queryID).name(), message.queryID, dals.loggingDal.getScriptDetails(message.queryID))
    }
  }
}
