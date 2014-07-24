package com.xpatterns.jaws.data.DTO
import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class Column(name: String, dataType: String) {

  override def hashCode(): Int = {
    val prime = 31;
    var result = 1;
    Option(name) match {
      case None => result = prime * result + 0
      case _ => result = prime * result + name.hashCode()
    }
    Option(dataType) match {
      case None => result = prime * result + 0
      case _ => result = prime * result + dataType.hashCode()
    }

    result
  }

  override def equals(other: Any): Boolean = {

    other match {

      case that: Column =>
        (that canEqual this) &&
         name == that.name &&
         dataType == that.dataType

      case _ => false
    }
  }
}	

object Column {
  implicit val columnJson = jsonFormat2(apply)
}