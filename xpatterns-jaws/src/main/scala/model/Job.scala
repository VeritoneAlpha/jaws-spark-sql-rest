package model

import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Job(state: String, uuid: String, description: String)

object Job {
  implicit val logJson = jsonFormat3(apply)
  
  
}