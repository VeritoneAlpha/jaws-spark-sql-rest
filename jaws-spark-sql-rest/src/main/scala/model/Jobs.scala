package model

import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Jobs (jobs : Array[Job])

object Jobs {
  implicit val jobsJson = jsonFormat1(apply)
  
  
}