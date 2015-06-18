package customs

import java.io.ByteArrayInputStream
import scala.io.Source
import customs.CommandsProcessor._
import com.xpatterns.jaws.data.DTO.Column
import org.apache.spark.sql.catalyst.expressions.Row
import com.xpatterns.jaws.data.utils.ResultsConverter
import org.apache.spark.sql.types._

class ResultsProcessor
object ResultsProcessor {
  val headerMatcher = "([^.]*?\\.)?(.+)".r

  def getLastResults(inputStream: ByteArrayInputStream): ResultsConverter = {
    val reader = Source.fromInputStream(inputStream)
    try {
      val lastCmdResults = reader getLines () dropWhile (!_.equals(QUERY_DELIMITATOR)) toList
      val headers = toStructType(getHeader(lastCmdResults(1)))
      val results = getResults(lastCmdResults drop 2)
      new ResultsConverter(headers, results)

    } finally if (reader != null) reader close ()
  }

  def getHeader(headerLine: String): Array[String] = {
    headerLine split "\t" map (column => headerMatcher replaceAllIn (column, m => m group 2))
  }

  def toStructType  (headers : Array[String]) : StructType = {
    val fields = headers map (column =>  new StructField(column, StringType, true))
    new StructType(fields)
  }
  
  def getResults(resultLines: List[String]): Array[Row] = {
    val resultsArray = resultLines map (line => line split "\t") toArray
    val result = resultsArray map (arr => Row.fromSeq(arr))
    result
  }
}