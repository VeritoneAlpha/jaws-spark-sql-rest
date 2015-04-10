import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import scala.util.Try
import customs.ResultsProcessor._
import org.scalatest.Matchers._
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import customs.CommandsProcessor._
import java.io.ByteArrayInputStream
import com.xpatterns.jaws.data.DTO.Result
import com.xpatterns.jaws.data.DTO.Column

@RunWith(classOf[JUnitRunner])
class ResultsProcessorTest extends FunSuite {

  test("getHeader : columns with .") {

    val headers = getHeader("mzzmjgycpp.name	mzzmjgycpp.age	mzzmjgycpp.sex")
    val requiredHeaders = Array("name", "age", "sex")

    headers should be(requiredHeaders)
  }

  test("getHeader : columns without .") {

    val headers = getHeader("name	age	sex")
    val requiredHeaders = Array("name", "age", "sex")

    headers should be(requiredHeaders)
  }

  test("getResults") {

    val results = getResults(List("name	age	sex", "name1	age1	sex1", "name2	age2	sex2"))
    val requiredResults = Array(Array("name", "age", "sex"), Array("name1", "age1", "sex1"), Array("name2", "age2", "sex2"))

    results should be(requiredResults)
  }

  test("get Last Results") {

    val stdOutOS = new ByteArrayOutputStream
    val osWriter = new OutputStreamWriter(stdOutOS)
    osWriter.write("db1\n")
    osWriter.write("db2\n")
    osWriter.write("db3\n")
    osWriter.write(s"$QUERY_DELIMITATOR\n")
    osWriter.write("mzzmjgycpp.name	mzzmjgycpp.age	mzzmjgycpp.sex\n")
    osWriter.write("name	age	sex\n")
    osWriter.write("name1	age1	sex1\n")
    osWriter.write("name2	age2	sex2")

    osWriter.flush()

    val results = getLastResults(new ByteArrayInputStream(stdOutOS.toByteArray()))
    val requiredResults = new Result(Array(Column("name", ""), Column("age", ""), Column("sex", "")), Array(Array("name", "age", "sex"), Array("name1", "age1", "sex1"), Array("name2", "age2", "sex2")))
    
    
    osWriter.close()
    assert(results.equals(requiredResults))
  }
  
   test("get Last Results - no results") {

    val stdOutOS = new ByteArrayOutputStream
    val osWriter = new OutputStreamWriter(stdOutOS)
    osWriter.write("db1\n")
    osWriter.write("db2\n")
    osWriter.write("db3\n")
    osWriter.write(s"$QUERY_DELIMITATOR\n")
    osWriter.write("mzzmjgycpp.name	mzzmjgycpp.age	mzzmjgycpp.sex\n")

    osWriter.flush()

    val results = getLastResults(new ByteArrayInputStream(stdOutOS.toByteArray()))
    val requiredResults = new Result(Array(Column("name", ""), Column("age", ""), Column("sex", "")), Array[Array[String]]())
    
    
    osWriter.close()
    assert(results.equals(requiredResults))
  }

}