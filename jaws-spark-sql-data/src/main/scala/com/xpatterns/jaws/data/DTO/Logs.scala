package com.xpatterns.jaws.data.DTO
import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Logs (logs : Array[Log], status: String)

object Logs {
  implicit val logsJson = jsonFormat2(apply)
}