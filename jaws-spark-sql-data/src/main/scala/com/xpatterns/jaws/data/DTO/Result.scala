package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.types.StructType

/**
 * Created by emaorhian
 */
case class Result(schema: Array[Column], results: Array[Array[String]]) {

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

      case that: Result =>
        (that canEqual this) &&
          results.deep == that.results.deep &&
          schema.deep == that.schema.deep

      case _ => false
    }
  }

  override def toString(): String = {
    "ResultDTO [schema=" + schema + ", results=" + results + "]"
  }

  def this(schema: StructType, result: Array[Row]) {
    this(Result.getSchema(schema), Result.getResults(result))
  }

  def this(schema: Array[Column], res: StructType) {
    this(schema, res.fields.map(field => Array(field.name.toString.trim, field.dataType.toString.trim)) toArray)
  }

  def this(schema: Array[Column], filteredResults: Array[Tuple2[Object, Array[Object]]]) {
    this(schema, filteredResults map { case (index, row) => row map (field => Option(field).getOrElse("Null").toString) })
  }

}

object Result {
  implicit val resultJson = jsonFormat2(apply)

  def trimResults(result: Result): Result = {
    Result(result.schema, result.results.map(row => row.map(field => field.trim())))
  }

  def getSchema(schema: StructType): Array[Column] = {
    schema.fields map (field => new Column(field.name, field.dataType.toString())) toArray
  }

  def getResults(results: Array[Row]): Array[Array[String]] = {
    results map (row => {
      row.map(value => {
        Option(value) match {
          case None => "Null"
          case _ => value.toString()
        }
      }) toArray
    })

  }
}  
  