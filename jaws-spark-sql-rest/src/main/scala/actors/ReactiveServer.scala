package actors

import akka.actor.ActorRef
import java.net.InetSocketAddress
import org.java_websocket.WebSocket
import org.java_websocket.server.WebSocketServer
import org.java_websocket.handshake.ClientHandshake
import akka.actor.actorRef2Scala

/**
 * Created by emaorhian
 */
object ReactiveServer {
  sealed trait ReactiveServerMessage
  case class Message(ws: WebSocket, msg: String)
    extends ReactiveServerMessage
  case class Open(uuid: String, ws: WebSocket, hs: ClientHandshake)
    extends ReactiveServerMessage
  case class Close(ws: Option[WebSocket], code: Int, reason: String, external: Boolean)
    extends ReactiveServerMessage
  case class Error(ws: Option[WebSocket], ex: Exception)
    extends ReactiveServerMessage

}
class ReactiveServer(val port: Int, val reactor: ActorRef)
  extends WebSocketServer(new InetSocketAddress(port)) {

  val urlPattern = """^\/jaws\/logs\?.*(?<=&|\?)uuid=([^&]+)""".r

  final override def onMessage(ws: WebSocket, msg: String) {
  }

  final override def onOpen(ws: WebSocket, hs: ClientHandshake) {
    Option(ws) match {
      case None => Configuration.log4j.debug("[ReactiveServer] the ws is null")
      case _ => {
        var description = hs.getResourceDescriptor()
        val urlPattern(uuid) = description
        reactor ! ReactiveServer.Open(uuid, ws, hs)
      }
    }
  }
  final override def onClose(ws: WebSocket, code: Int, reason: String, external: Boolean) {
    Option(ws) match {
      case None => Configuration.log4j.debug("[ReactiveServer] the ws is null")
      case _ => {
        reactor ! ReactiveServer.Close(Option(ws), code, reason, external)

      }
    }
  }
  final override def onError(ws: WebSocket, ex: Exception) {
    reactor ! ReactiveServer.Error(Option(ws), ex)

  }
}
