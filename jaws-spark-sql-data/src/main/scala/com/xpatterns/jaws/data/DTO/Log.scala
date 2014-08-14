package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol._
import java.util.Collection
import scala.Array.canBuildFrom
import spray.json._

/**
 * Created by emaorhian
 */
case class Log(log: String, queryID: String, timestamp: Long){

}

object Log {
  implicit val logJson = jsonFormat3(apply) 
  
  def getLogArray(logs : Collection[Log]): Array[Log]  = {
    var scalaLogs =  Array[Log]()
    var it = logs.iterator()
    while (it.hasNext()){
      var logDto = it.next()
      scalaLogs = scalaLogs ++ Array(Log(logDto.log, logDto.queryID, logDto.timestamp))
    }
    scalaLogs
  }
}