package com.xpatterns.jaws.data.DTO
import spray.json.DefaultJsonProtocol._

case class ParquetTable(name: String, filePath: String){
   def this() = {
     this("","")
   } 
}
object ParquetTable {
  implicit val logJson = jsonFormat2(apply)
}