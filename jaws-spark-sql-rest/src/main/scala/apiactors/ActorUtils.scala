package apiactors

import akka.actor.ActorRef
import messages.ErrorMessage

import scala.util.{Failure, Success, Try}

/**
 * Created by emaorhian
 */

object ActorsPaths {
  def SUPERVISOR_ACTOR_NAME = "DomainSupervisor"
  def SUPERVISOR_ACTOR_PATH = s"/user/$SUPERVISOR_ACTOR_NAME"

  def GET_QUERIES_ACTOR_NAME = "GetQueries"
  def GET_QUERIES_ACTOR_PATH = s"$SUPERVISOR_ACTOR_PATH/$GET_QUERIES_ACTOR_NAME"

  def GET_TABLES_ACTOR_NAME = "GetTables"
  def GET_TABLES_ACTOR_PATH = s"$SUPERVISOR_ACTOR_PATH/$GET_TABLES_ACTOR_NAME"

  def RUN_SCRIPT_ACTOR_NAME = "RunScript"
  def RUN_SCRIPT_ACTOR_PATH = s"$SUPERVISOR_ACTOR_PATH/$RUN_SCRIPT_ACTOR_NAME"

  def GET_LOGS_ACTOR_NAME = "GetLogs"
  def GET_LOGS_ACTOR_PATH = s"$SUPERVISOR_ACTOR_PATH/$GET_LOGS_ACTOR_NAME"

  def GET_RESULTS_ACTOR_NAME = "GetResults"
  def GET_RESULTS_ACTOR_PATH = s"$SUPERVISOR_ACTOR_PATH/$GET_RESULTS_ACTOR_NAME"

  def GET_DATABASES_ACTOR_NAME = "GetDatabases"
  def GET_DATABASES_ACTOR_PATH = s"$SUPERVISOR_ACTOR_PATH/$GET_DATABASES_ACTOR_NAME"
}

object ActorOperations {
  def returnResult (tryResult : Try[Any], results : Any, errorMessage : String, senderActor: ActorRef){
    tryResult match {
      case Success(v) => senderActor ! results
      case Failure(e) => senderActor ! ErrorMessage(s"$errorMessage ${e.getMessage}")
    }
  }
}