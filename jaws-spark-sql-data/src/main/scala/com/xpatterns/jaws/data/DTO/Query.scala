package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Query(state: String, queryID: String, query: String, metaInfo : QueryMetaInfo)

object Query {
  implicit val logJson = jsonFormat4(apply)
}