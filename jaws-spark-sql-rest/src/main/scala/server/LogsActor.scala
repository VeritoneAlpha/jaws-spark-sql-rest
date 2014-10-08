package server

import akka.actor.{ Actor, ActorLogging }
import scala.collection._
import org.java_websocket.WebSocket
import server.ReactiveServer.Close
import server.ReactiveServer.Error
import server.ReactiveServer.Open
import akka.actor.actorRef2Scala

/**
 * Created by emaorhian
 */
object LogsActor {
  sealed trait LogsMessage

  case class Unregister(ws: Option[WebSocket]) extends LogsMessage
  case class PushLogs(uuid: String, msg: String) extends LogsMessage

}

class LogsActor extends Actor with ActorLogging {
  import LogsActor._
  import server.ReactiveServer._

  val uuidToClients = mutable.Map[String, mutable.ListBuffer[WebSocket]]()

  override def receive = {
    case Open(uuid, ws, hs) => {
      var webSockets = mutable.ListBuffer[WebSocket]()
      uuidToClients.get(uuid) match {
        case None => webSockets = mutable.ListBuffer[WebSocket]()
        case Some(wss) => webSockets = wss
      }
      uuidToClients += ((uuid, (webSockets :+ ws)))
      log.info("registered monitor for {}", ws.getLocalSocketAddress())
    }

    case Close(ws, code, reason, ext) => self ! Unregister(ws)

    case Error(ws, ex) => self ! Unregister(ws)

    case PushLogs(uuid, msg) =>
      log.debug("received msg '{}'", msg)
      val webSockets = uuidToClients.get(uuid)
      webSockets match {
        case None => log.debug("There is no such uuid")
        case Some(wss) => wss.foreach(ws => ws.send(msg))
      }

    case Unregister(ws) => {
      ws match {
        case None => log.info("There is nothing to unregister")
        case Some(wss) =>
          log.info("unregister monitor")
          uuidToClients.foreach(tuple => {
            val clients = tuple._2
            clients -= wss
            uuidToClients.put(tuple._1, clients)
          })
      }

    }
  }
}
