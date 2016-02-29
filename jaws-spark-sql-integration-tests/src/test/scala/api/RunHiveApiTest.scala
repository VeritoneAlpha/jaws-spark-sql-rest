package api

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import foundation.TestBase


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