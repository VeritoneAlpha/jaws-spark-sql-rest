package server

import akka.actor.{ActorSystem, Props, ActorRef}
import apiactors.ActorsPaths._
import apiactors.ActorsPaths
import scala.concurrent.Await
import akka.util.Timeout
import akka.pattern.ask

/**
 * Created by emaorhian
 */
object MainActors {
  //  self: Systems =>
  val localSystem: ActorSystem = ActorSystem("localSystem")
  val remoteSystem: ActorSystem = ActorSystem("remoteSystem", Configuration.remote)
  val localSupervisor = localSystem.actorOf(Props(classOf[Supervisor]), ActorsPaths.LOCAL_SUPERVISOR_ACTOR_NAME)
  val remoteSupervisor = remoteSystem.actorOf(Props(classOf[Supervisor]), ActorsPaths.REMOTE_SUPERVISOR_ACTOR_NAME)
  val logsActor = createActor(Props(new LogsActor), LOGS_WEBSOCKETS_ACTOR_NAME, localSupervisor)


  def createActor(props: Props, name: String, supervisor: ActorRef): ActorRef = {
    implicit val timeout = Timeout(Configuration.timeout)
    val future = ask(supervisor, (props, name))
    val actor = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
    actor
  }
}


