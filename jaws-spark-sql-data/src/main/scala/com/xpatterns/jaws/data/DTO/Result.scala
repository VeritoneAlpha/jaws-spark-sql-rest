package com.xpatterns.jaws.data.DTO

import spray.json.DefaultJsonProtocol._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.expressions.AttributeSet

/**
 * Created by emaorhian
 */
case class Result(schema: Array[Column], results: Array[Array[String]]) {

   def this() = {
     this(Array[Column](),Array[Array[String]]())
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

  def this(schema: AttributeSet, result: Array[Row]) {
	  this(Result.getSchema(schema), Result.getResults(result))
  }
}

object Result {
  implicit val logsJson = jsonFormat2(apply)

  def fromTuples(schema: Array[Column], filteredResults: Array[Tuple2[Object, Array[Object]]]): Result = {

    var results = Array[Array[String]]()
    filteredResults.foreach(tuple => {
      var row = Array[String]()
      tuple._2.foreach(field => row = row ++ Array(Option(field).getOrElse("Null").toString))
      results = results ++ Array(row)
    })
    Result(schema, results)
  }

  def trimResults(result: Result): Result = {
    Result(result.schema, result.results.map(row => row.map(field => field.trim())))
  }
  
   def getSchema(schema: AttributeSet): Array[Column] = {
    var finalSchema = Array[Column]()
    schema.foreach(attribute => { finalSchema = finalSchema ++ Array(new Column(attribute.name, attribute.dataType.toString())) })
    finalSchema
  }
   
   def getResults(results: Array[Row]): Array[Array[String]] = {
    var finalResults = Array[Array[String]]()
 
    results.foreach(row => {
      var rrow = row.map(value =>{
        Option(value) match {
            case None => "Null"
            case _ => value.toString()
      }})
      
      finalResults = finalResults ++ Array(rrow.toArray)
    })
    
    finalResults
  }
}  
  