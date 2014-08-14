package model
import spray.json.DefaultJsonProtocol._
import scala.collection.JavaConverters._
import scala.Array.canBuildFrom
import shark.api.ResultSet
import shapeless.ToList
import actors.Configuration
import com.xpatterns.jaws.data.DTO.ResultDTO
import com.xpatterns.jaws.data.DTO.Column

/**
 * Created by emaorhian
 */
case class Result(schema: Array[Column], results: Array[Array[String]]) {
  def getSchema(): Array[Column] = {
    schema
  }

  def getResults(): Array[Array[String]] = {
    results
  }

  def toDTO(): ResultDTO = {
    val js = schema
    val result = results
    
    return new ResultDTO(js, result)
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

  def fromResultDTO(result: ResultDTO): Result = {
    var schema = Array[Column]()
    var results = Array[Array[String]]()
    for (i <- 0 to result.schema.length - 1) {
      schema = schema ++ List(result.schema(i))
    }

    for (rowIndex <- 0 to result.results.length - 1) {
      var row = Array[String]()
      var rrow = result.results(rowIndex)
      for (fieldIndex <- 0 to rrow.length - 1) {
        row = row ++ Array(rrow(fieldIndex))
      }
      results = results ++ Array(row)
    }

    Result(schema, results)
  }

  def fromResultSet(resultSet: ResultSet): Result = {
    var schema = Array[Column]()
    var results = Array[Array[String]]()

    Option(resultSet) match {
      case None => Configuration.log4j.info("Result set is null")
      case _ => {
        // add schema
        resultSet.schema.foreach(desc => schema = schema ++ Array(new Column(desc.name, desc.dataType.name)))
        // add results
        resultSet.results.foreach(res => results = results ++ Array(res.map(value => {
          Option(value) match {
            case None => "Null"
            case _ => value.toString()
          }
        })))
       
      }
    }
    Result(schema, results)
  }

  def trimResults(result: Result): Result = {
    Result(result.schema, result.results.map(row => row.map(field => field.trim())))
  }
}  
  