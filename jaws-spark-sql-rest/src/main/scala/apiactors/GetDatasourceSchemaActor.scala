package apiactors

import akka.actor.Actor
import apiactors.ActorOperations._
import messages.GetDatasourceSchemaMessage
import server.Configuration

import scala.util.Try

/**
 * Created by lucianm on 06.02.2015.
 */
class GetDatasourceSchemaActor extends Actor {

  def receive = {
    case request: GetDatasourceSchemaMessage =>

      val message: String = s"Getting the datasource schema for path ${request.path}, sourceType ${request.sourceType}, storageType ${request.storageType}"
      Configuration.log4j.info(message)

      /*instead of creating SJR context and launching the job,
        just provide the schema
       */
      val response = Try(message)

      // respond to the "ask" request
      returnResult(response, message, "GET datasource schema failed with the following message: ", sender)
    case request: Any => Configuration.log4j.error(request.toString)
  }

}
