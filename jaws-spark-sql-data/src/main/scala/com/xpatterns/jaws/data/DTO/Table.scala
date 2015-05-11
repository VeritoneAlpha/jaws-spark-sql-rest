package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol._


/**
 * Created by emaorhian
 */
case class Table(name: String, columns: Array[TableColumn], extraInfo : Array[Array[String]])

object Table {
  implicit val logJson = jsonFormat3(apply)

}