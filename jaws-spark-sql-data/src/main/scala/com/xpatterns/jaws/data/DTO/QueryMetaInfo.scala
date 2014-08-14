package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol._
import java.util.Collection
import scala.Array.canBuildFrom
import spray.json._

/**
 * Created by emaorhian
 */
case class QueryMetaInfo(nrOfResults : Long, maxNrOfResults : Long, resultsInCassandra : Boolean, isLimited : Boolean){
  
   def this() = {
     this(0,0,false,false)
   }
   
}

object QueryMetaInfo {
  implicit val logJson = jsonFormat4(apply) 
}