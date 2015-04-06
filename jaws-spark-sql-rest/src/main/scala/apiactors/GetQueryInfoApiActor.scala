package apiactors

import apiactors.ActorOperations._
import messages.GetQueryInfoMessage
import com.xpatterns.jaws.data.contracts.DAL
import server.Configuration
import akka.actor.Actor
import com.google.common.base.Preconditions
import com.xpatterns.jaws.data.DTO.Query

import scala.util.Try

/**
 * Created by emaorhian
 */
class GetQueryInfoApiActor(dals: DAL) extends Actor {

  override def receive = {

    case message: GetQueryInfoMessage => {
      Configuration.log4j.info("[GetQueryInfoApiActor]: retrieving the query information for " + message.queryID)

      var resultQuery: Query = null;
      val tryGetQueryInfo = Try {
        
        resultQuery = new Query(dals.loggingDal.getState(message.queryID).toString, message.queryID, dals.loggingDal.getScriptDetails(message.queryID), dals.loggingDal.getMetaInfo(message.queryID))
      }
      returnResult(tryGetQueryInfo, resultQuery, "GET query info failed with the following message: ", sender)
    }
  }
}
