package api

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import foundation.TestBase
import com.xpatterns.jaws.data.utils.Utils
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FileSystem
import java.io.File
import org.apache.hadoop.fs.Path
import akka.io.IO
import akka.pattern.ask
import spray.can.Http
import spray.http._
import spray.client.pipelining._
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.duration.Duration._
import scala.util.Success
import scala.util.Failure
import scala.collection.GenSeq

@RunWith(classOf[JUnitRunner])
class RunHiveApiTest extends TestBase {

  test(" select count ") {

    val url = s"${jawsHiveUrl}run?limit=10"
    val body = s"use $database;\nselect count(*) from $table"

    val queryId = postRun(url, body)
    val queryStatus = waitforCompletion(queryId, 100)
    assert(queryStatus === "DONE", "Query is not DONE!")
    val results = getResults(queryId, 0, 200)
    assert(1 === results.result.length, "Different number of rows")
    assert(1 === results.result(0).length, "Different number of rows2")
    assert("6" === results.result(0)(0), "Different count")
  }

  test(" select * limited") {

    val url = s"${jawsHiveUrl}run?"
    val queryID = selectAllFromTable(url, table)
    validataAllResultsFromNormalTable(queryID, true)
  }

  test(" select * unlimited") {

    val url = s"${jawsHiveUrl}run"
    val queryID = selectAllFromTable(url, table)
    validataAllResultsFromNormalTable(queryID, true)
  }
}