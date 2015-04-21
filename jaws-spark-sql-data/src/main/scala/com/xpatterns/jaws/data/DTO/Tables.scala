package com.xpatterns.jaws.data.DTO

import scala.collection.JavaConverters._
import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Tables(tables: Map[String, Map[String, Result]])

object Tables {
  implicit val tablesJson = jsonFormat1(apply)
}