package apiactors

import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.ActorSelection
import server.Configuration
import messages.CancelMessage
import server.JawsController
import apiactors.ActorsPaths._
import messages.RegisterTableMessage
import akka.pattern._
import akka.util.Timeout
import messages.UnregisterTableMessage

class BalancerActor extends Actor {
  var runActors: Array[ActorSelection] = null
  var registerParquetTableActors: Array[ActorSelection] = null
  implicit val timeout = Timeout(Configuration.timeout.toInt)

  if (!Configuration.remoteDomainActor.getOrElse("").isEmpty) {
    Configuration.log4j.info(s"There are remote actors at: ${Configuration.remoteDomainActor}")
    runActors = for (actorIp <- Configuration.remoteDomainActor.get.split(",")) yield context.actorSelection(s"$REMOTE_ACTOR_SYSTEM_PREFIX_PATH$actorIp$RUN_SCRIPT_ACTOR_PATH")
    registerParquetTableActors = for (actorIp <- Configuration.remoteDomainActor.get.split(",")) yield context.actorSelection(s"$REMOTE_ACTOR_SYSTEM_PREFIX_PATH$actorIp$REGISTER_PARQUET_TABLE_ACTOR_PATH")
  }

  def receive = {
    case message: CancelMessage =>
      JawsController.runScriptActor ! message
      Option(runActors) match {
        case None => Configuration.log4j.info("[BalancerActor] There aren't any remote run actors to send the cancel message to!")
        case _ => runActors.foreach { dom => dom ! message }
      }

    case message @ (_: RegisterTableMessage | _: UnregisterTableMessage) => {
      Option(registerParquetTableActors) match {
        case None => Configuration.log4j.info("[BalancerActor] There aren't any remote register parquet actors to send the register table message to!")
        case _ => registerParquetTableActors.foreach { dom =>
          {
            Configuration.log4j.info(s"Sending message to the registering actor at ${dom}")
            dom ! message
          }
        }
      }

      sender ! JawsController.registerParquetTableActor ? message
    }

  }

}