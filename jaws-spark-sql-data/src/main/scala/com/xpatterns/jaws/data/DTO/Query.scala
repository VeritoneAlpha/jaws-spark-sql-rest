package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Query(state: String, queryID: String, query: String)

object Query {
  implicit val logJson = jsonFormat3(apply)

}