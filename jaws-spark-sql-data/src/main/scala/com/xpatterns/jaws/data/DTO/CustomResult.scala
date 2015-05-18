package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol._

case class CustomResult(schema: Array[Column], result: Array[Array[String]]) {

  def this() = {
    this(Array.empty, Array.empty)
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    Option(result) match {
      case None => result = prime * result + 0
      case _ => result = prime * result + result.hashCode()
    }
    Option(schema) match {
      case None => result = prime * result + 0
      case _ => result = prime * result + schema.hashCode()
    }

    result
  }

  override def equals(other: Any): Boolean = {

    other match {

      case that: CustomResult =>
        (that canEqual this) &&
          result.deep == that.result.deep &&
          schema.deep == that.schema.deep

      case _ => false
    }
  }
}

object CustomResult {
  implicit val customResultJson = jsonFormat2(apply) 
  }