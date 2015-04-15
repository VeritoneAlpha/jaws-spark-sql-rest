package customs

import java.io.ByteArrayInputStream
import scala.io.Source
import customs.CommandsProcessor._
import com.xpatterns.jaws.data.DTO.Column
import com.xpatterns.jaws.data.DTO.Result

class ResultsProcessor
object ResultsProcessor {
  val headerMatcher = "([^.]*?\\.)?(.+)".r

  def getLastResults(inputStream: ByteArrayInputStream) :Result= {
    val reader = Source.fromInputStream(inputStream)
    try {
      val lastCmdResults = reader getLines () dropWhile (!_.equals(QUERY_DELIMITATOR)) toList
      val headers = getHeader(lastCmdResults(1)) map (columnName => Column(columnName, ""))
      val results = getResults(lastCmdResults drop 2)
      new Result(headers, results)
     

    } finally if (reader != null) reader close ()
  }

  def getHeader(headerLine: String): Array[String] = {
    headerLine split "\t" map (column => headerMatcher replaceAllIn (column, m => m group 2))
  }

  def getResults(resultLines: List[String]): Array[Array[String]] = {
    resultLines map (line => line split "\t") toArray
  }
}