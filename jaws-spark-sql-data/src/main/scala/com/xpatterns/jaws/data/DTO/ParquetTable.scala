package com.xpatterns.jaws.data.DTO
import spray.json.DefaultJsonProtocol._

case class ParquetTable(name: String, filePath: String, namenode : String){
   def this() = {
     this("","","")
   } 
}
object ParquetTable {
  implicit val logJson = jsonFormat3(apply)
}