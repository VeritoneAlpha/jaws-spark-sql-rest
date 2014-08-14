package com.xpatterns.jaws.data.DTO

import scala.collection.JavaConverters._
import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Tables(tables: Map[String, Map[String, ResultDTO]])

object Tables {
  implicit val tablesJson = jsonFormat1(apply)

  def fromJavaMap(tables: java.util.Map[String, java.util.Map[String, ResultDTO]]): Tables = {
    val myScalaMap = tables.asScala.mapValues(_.asScala.toMap).toMap
    Tables(myScalaMap)
  }
  
  def fromMutableMap(tables: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, ResultDTO]]): Tables = {
    val myScalaMap = tables.mapValues(_.toMap).toMap
    Tables(myScalaMap)
  }
}