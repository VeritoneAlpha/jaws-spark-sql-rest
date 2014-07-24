package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol.arrayFormat
import spray.json.DefaultJsonProtocol.jsonFormat1

/**
 * Created by emaorhian
 */
case class Queries (queries : Array[Query])

object Queries {
  implicit val  queriesJson = jsonFormat1(apply)
  
  
}