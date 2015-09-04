package api

import com.google.gson.Gson
import com.xpatterns.jaws.data.DTO.Logs
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import foundation.TestBase
import com.xpatterns.jaws.data.utils.Utils
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FileSystem
import java.io.File
import org.apache.hadoop.fs.Path
import spray.http._
import scala.concurrent.Await
import scala.concurrent.duration.Duration._
import scala.util.Success
import scala.util.Failure
import scala.collection.GenSeq

@RunWith(classOf[JUnitRunner])
class RunApiTest extends TestBase {

  override def beforeAll() {
    println("Uploading the file used for tests on hdfs")
    Utils.createFolderIfDoesntExist(hadoopConf, hdfsInputFolder, true)
    val fs = FileSystem.newInstance(hadoopConf)
    val file = new File(getClass().getResource("/people.txt").getPath())
    FileUtil.copy(file, fs, new Path(hdfsInputFolder), false, hadoopConf)
  }

  test(" create test table ") {

    val username = System.getProperties().get("user.name")
    val url = s"${jawsUrl}run?limited=true"
    val createDatabaseStm = s"create database if not exists $database;\n use $database;\n"
    val createTableStm = s" drop table if exists $table;\ncreate external table $table (name String, age Int, sex String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/$username/$hdfsInputFolder';"
    val body = s"$createDatabaseStm$createTableStm"

    val queryId = postRun(url, body)
    val queryStatus = waitforCompletion(queryId, 100)
    assert(queryStatus === "DONE", "Query is not DONE!")
  }

  test(" select count ") {

    val url = s"${jawsUrl}run?limited=true"
    val body = s"use $database;\nselect count(*) from $table"

    val queryId = postRun(url, body)
    val queryStatus = waitforCompletion(queryId, 100)
    assert(queryStatus === "DONE", "Query is not DONE!")
    val results = getResults(queryId, 0, 200)
    assert(1 === results.result.length, "Different number of rows")
    assert(1 === results.result(0).length, "Different number of rows2")
    assert(6.0 === results.result(0)(0), "Different count")
  }

  test(" select * limited") {
    val url = s"${jawsUrl}run?limited=true"
    val queryID= selectAllFromTable(url, table)
    validataAllResultsFromNormalTable(queryID, false)

  }

  test(" select * unlimited hdfs") {
    val url = s"${jawsUrl}run?limited=false&destination=hdfs"
    val queryID= selectAllFromTable(url, table)
    validataAllResultsFromNormalTable(queryID, false)
  }

  test(" select * unlimited tachyon") {
    if (runTachyon) {
      val url = s"${jawsUrl}run?limited=false&destination=tachyon"
      val queryID = selectAllFromTable(url, table)
      validataAllResultsFromNormalTable(queryID, false)
    } else println("Tachyon tests are ignored")
  }

  test(" select * limited with limit") {
    val url = s"${jawsUrl}run?limited=true&numberOfResults=2"
    val body = s"use $database;\nselect * from $table"

    val queryId = postRun(url, body)
    val queryStatus = waitforCompletion(queryId, 100)
    assert(queryStatus === "DONE", "Query is not DONE!")
    val results = getResults(queryId, 0, 200)

    assert(2 === results.result.length, "Different number of rows")
    assert(3 === results.result(0).length, "Different number of rows2")
  }

  test(" select * limited with limit in query") {
    val url = s"${jawsUrl}run?limited=true"
    val body = s"use $database;\nselect * from $table limit 3"

    val queryId = postRun(url, body)
    val queryStatus = waitforCompletion(queryId, 100)
    assert(queryStatus === "DONE", "Query is not DONE!")
    val results = getResults(queryId, 0, 200)
    assert(3 === results.result.length, "Different number of rows")
    assert(3 === results.result(0).length, "Different number of rows2")
  }

  test(" select distinct") {
    val url = s"${jawsUrl}run?limited=true"
    val body = s"use $database;\nselect distinct name from $table"

    val queryId = postRun(url, body)
    val queryStatus = waitforCompletion(queryId, 100)
    assert(queryStatus === "DONE", "Query is not DONE!")
    val results = getResults(queryId, 0, 200)
    val flatResults = results.result.flatMap(x => x)
    assert(6 === results.result.length, "Different number of rows")
    assert(1 === results.result(0).length, "Different number of columns")
    assert(flatResults.contains("Ana"), "Ana is missing")
    assert(flatResults.contains("George"), "George is missing")
    assert(flatResults.contains("Alina"), "Alina is missing")
    assert(flatResults.contains("Paul"), "Paul is missing")
    assert(flatResults.contains("Pavel"), "Pavel is missing")
    assert(flatResults.contains("Ioana"), "Ioana is missing")

  }

  test(" group by") {
    val url = s"${jawsUrl}run?limited=true"
    val body = s"use $database;\nselect count(name), sex from  $table group by sex"

    val queryId = postRun(url, body)
    val queryStatus = waitforCompletion(queryId, 100)
    assert(queryStatus === "DONE", "Query is not DONE!")
    val results = getResults(queryId, 0, 200)
    val flatResults = results.result.flatMap(x => x)
    assert(2 === results.result.length, "Different number of rows")
    assert(2 === results.result(0).length, "Different number of columns")
    assert(flatResults.containsSlice(GenSeq(3, "m")), "Different nb of men")
    assert(flatResults.containsSlice(GenSeq(3, "f")), "Different nb of women")

  }

  test("logs api") {
    val url = s"${jawsUrl}run?limited=true"
    val body = s"use $database;\nselect count(name), sex from  $table group by sex"

    val queryId = postRun(url, body)
    val queryStatus = waitforCompletion(queryId, 100)
    assert(queryStatus === "DONE", "Query is not DONE!")

    val response = get(s"${jawsUrl}logs?queryID=$queryId&limit=100")

    Await.ready(response, Inf).value.get match {
      case Success(r: HttpResponse) =>
        assert(r.status.isSuccess)
        val responseText = r.entity.data.asString
        val gson = new Gson()
        val logs = gson.fromJson(responseText, classOf[Logs])
        assert(logs.status === "DONE", "Query is not done!")
        assert(logs.logs.nonEmpty, "The logs are empty")

      case Failure(e) =>
        println(e.getMessage)
        fail()
    }
  }
}