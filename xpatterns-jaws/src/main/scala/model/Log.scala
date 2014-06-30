package model

import spray.json.DefaultJsonProtocol._
import java.util.Collection
import com.xpatterns.jaws.data.DTO.LogDTO
import scala.Array.canBuildFrom

/**
 * Created by emaorhian
 */
case class Log(log: String, jobId: String, timestamp: Long)

object Log {
  implicit val logJson = jsonFormat3(apply)
  
  def getLogArray(logs : Collection[LogDTO]): Array[Log]  = {
    var scalaLogs =  Array[Log]()
    var it = logs.iterator()
    while (it.hasNext()){
      var logDto = it.next()
      scalaLogs = scalaLogs ++ Array(Log(logDto.log, logDto.jobId, logDto.timestamp))
    }
    scalaLogs
  }
  
}