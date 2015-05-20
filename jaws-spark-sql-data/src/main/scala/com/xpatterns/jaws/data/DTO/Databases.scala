package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Databases(databases: Array[String])

object Databases {
  implicit val databasesJson = jsonFormat1(apply)
}