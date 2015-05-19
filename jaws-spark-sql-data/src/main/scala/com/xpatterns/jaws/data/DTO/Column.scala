package com.xpatterns.jaws.data.DTO
import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

/**
 * Created by emaorhian
 */
case class Column(name: String, dataType: String, comment: String, members: Array[Column]) {

  def this() = {
    this("", "", "", Array.empty)
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    Option(name) match {
      case None => result = prime * result + 0
      case _    => result = prime * result + name.hashCode()
    }
    Option(dataType) match {
      case None => result = prime * result + 0
      case _    => result = prime * result + dataType.hashCode()
    }
    Option(comment) match {
      case None => result = prime * result + 0
      case _    => result = prime * result + comment.hashCode()
    }
    Option(members) match {
      case None => result = prime * result + 0
      case _    => result = prime * result + members.hashCode()
    }

    result
  }

  override def equals(other: Any): Boolean = {

    other match {

      case that: Column =>
        (that canEqual this) &&
          name == that.name &&
          dataType == that.dataType &&
          comment == that.comment &&
          members.deep == that.members.deep

      case _ => false
    }
  }
}

object Column {
  implicit val columnJsonFormat: JsonFormat[Column] = lazyFormat(jsonFormat(Column.apply, "name", "dataType","comment", "members"))
}