package com.xpatterns.jaws.data.DTO

import scala.collection.JavaConverters._
import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Tables(database: String, tables: Array[Table])

object Tables {
  implicit val tablesJson = jsonFormat2(apply)
}