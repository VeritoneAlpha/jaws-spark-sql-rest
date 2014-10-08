package apiactors

import server.Configuration
import akka.actor.{Actor, ActorRef, ActorSelection, actorRef2Scala}
import messages.CancelMessage

/**
 * Created by emaorhian
 */
class CancelActor(domainActor: ActorRef) extends Actor {
  var remoteDomain: Array[ActorSelection] = null

  if (!Configuration.remoteDomainActor.getOrElse("").isEmpty) {
    Configuration.log4j.info("There are remote cancel actors to send the cancel message to: " + Configuration.remoteDomainActor)
    remoteDomain = for (actorIp <- Configuration.remoteDomainActor.get.split(",")) yield context.actorSelection(actorIp)
  }

  def receive = {
    case mes: CancelMessage =>
      domainActor ! mes
      Option(remoteDomain) match {
        case None => Configuration.log4j.info("[CancelActor] There aren't any remote domains to send the cancel message to!")
        case _ => remoteDomain.foreach { dom => dom ! mes }
      }
  }

}