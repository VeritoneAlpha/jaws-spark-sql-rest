package server
import akka.actor.Actor
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._
import scala.concurrent.duration._
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.actor.ActorSystem
 
/**
 * Created by emaorhian
 */
class Supervisor extends Actor {

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: Exception => Resume
    }
 
  def receive = {     
      case (p: Props, name: String, customSystem: ActorSystem) => sender ! customSystem.actorOf(p, name)
  }

}