package model

import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Queries (queries : Array[Query])

object Queries {
  implicit val  queriesJson = jsonFormat1(apply)
  
  
}