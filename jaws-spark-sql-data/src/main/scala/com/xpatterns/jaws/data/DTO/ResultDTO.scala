package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol._

/**
 * Created by emaorhian
 */
case class ResultDTO(var schema: Array[Column], var results: Array[Array[String]]) {

  def this() = {
    this(Array[Column](), Array[Array[String]]())
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    Option(results) match {
      case None => result = prime * result + 0
      case _ => result = prime * result + results.hashCode()
    }
    Option(schema) match {
      case None => result = prime * result + 0
      case _ => result = prime * result + schema.hashCode()
    }

    result
  }

  override def equals(other: Any): Boolean = {

    other match {

      case that: ResultDTO =>
        (that canEqual this) &&
          results.deep == that.results.deep &&
          schema.deep == that.schema.deep

      case _ => false
    }
  }

  override def toString(): String = {
    "ResultDTO [schema=" + schema + ", results=" + results + "]"
  }

}

object ResultDTO {
  implicit val logsJson = jsonFormat2(apply)

  def fromTuples(schema: Array[Column], filteredResults: Array[Tuple2[Object, Array[Object]]]): ResultDTO = {

    var results = Array[Array[String]]()
    filteredResults.foreach(tuple => {
      var row = Array[String]()
      tuple._2.foreach(field => row = row ++ Array(Option(field).getOrElse("Null").toString))
      results = results ++ Array(row)
    })
    ResultDTO(schema, results)
  }

  def trimResults(result: ResultDTO): ResultDTO = {
    ResultDTO(result.schema, result.results.map(row => row.map(field => field.trim())))
  }
}  
  