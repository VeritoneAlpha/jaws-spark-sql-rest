package actors

import akka.actor.Actor
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._
import scala.concurrent.duration._
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.actor.ActorSystem
import akka.actor.ActorRef
import messages.CancelMessage
import akka.actor.ActorSelection

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
        case _ => remoteDomain.map { dom => dom ! mes; dom }
      }
  }

}