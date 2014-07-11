package model

import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Queries (queries : Array[Query])

object Queries {
  implicit val jobsJson = jsonFormat1(apply)
  
  
}