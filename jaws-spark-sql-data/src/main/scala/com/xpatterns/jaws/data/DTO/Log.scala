package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol._


/**
 * Created by emaorhian
 */
case class Log(log: String, jobID: String, timestamp: Long)

object Log {
  implicit val logJson = jsonFormat3(apply) 
}