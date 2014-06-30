package api

import messages.GetDescriptionMessage
import traits.DAL
import actors.Configuration
import akka.actor.Actor
import com.google.common.base.Preconditions
/**
 * Created by emaorhian
 */
class GetDescriptionApiActor (dals: DAL) extends Actor{
  
  override def receive = {
    
  	case message : GetDescriptionMessage => {
      Configuration.log4j.info("[GetDescriptionApiActor]: retrieving the description for " + message.uuid)
		Preconditions.checkArgument(message.uuid != null && !message.uuid.isEmpty(), Configuration.UUID_EXCEPTION_MESSAGE)
		sender ! dals.loggingDal.getScriptDetails(message.uuid)
    }
  }
}
