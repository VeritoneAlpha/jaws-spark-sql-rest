package actors

import akka.actor.Props
import akka.actor.ActorSystem
import scala.concurrent.Await
import akka.util.Timeout
import akka.actor.ActorRef
import akka.pattern.ask

/**
 * Created by emaorhian
 */
trait MainActors {
  self: Systems => val logsActor = system.actorOf(Props[LogsActor], "LogsAct")
  val supervisor = system.actorOf(Props(classOf[Supervisor]), "DomainSupervisor")
  
  
  def createActor(props: Props, name: String, customSystem: ActorSystem): ActorRef = {
    implicit val timeout = Timeout(Configuration.timeout)
    val future = ask(supervisor, (props, name, customSystem))
    val actor = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
    actor
  }
}


