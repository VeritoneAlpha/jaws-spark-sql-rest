package foundation

import spray.client.pipelining._
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration._
import scala.util.Success
import scala.util.Failure
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import com.typesafe.config.ConfigFactory
import com.xpatterns.jaws.data.DTO.{Queries, CustomResult}
import spray.httpx.encoding.{ Gzip, Deflate }
import spray.httpx.SprayJsonSupport._
import scala.collection.GenSeq
import spray.http._
import com.google.gson.Gson

class TestBase extends FunSuite with BeforeAndAfterAll {
  implicit val system = ActorSystem()
  import system.dispatcher // execution context for futures

  private val conf = ConfigFactory.load
  conf.checkValid(ConfigFactory.defaultReference)

  val appConf = conf.getConfig("appConf")
  val jawsUrl = appConf.getString("jawsUrl")
  val jawsHiveUrl = appConf.getString("jawsHiveUrl")
  val namenodeIp = appConf.getString("namenodeIp")
  val hdfsInputFolder = appConf.getString("hdfsInputFolder")
  val database = appConf.getString("database")
  val table = appConf.getString("table")
  val runTachyon = appConf.getBoolean("runTachyon")
  val parquetFolder = appConf.getString("parquetFolder")
  val parquetTable = appConf.getString("parquetTable")

  val hadoopConf = getHadoopConf
  def getHadoopConf(): org.apache.hadoop.conf.Configuration = {
    val configuration = new org.apache.hadoop.conf.Configuration()
    configuration.set("fs.defaultFS", s"hdfs://$namenodeIp:8020")
    configuration
  }

  def postRun(url: String, body: String) = {
    val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
    var queryID = ""

    val response: Future[HttpResponse] = pipeline(Post(url, body))
    Await.ready(response, Inf).value.get match {
      case Success(r: HttpResponse) => {
        assert(r.status.isSuccess)
        queryID = r.entity.data.asString
        assert(queryID.isEmpty() == false, "Didn't receive a queryId")
      }
      case Failure(e) => {
        println(e.getMessage)
        fail()
      }
    }
    queryID
  }

  def post(url: String, body: String) = {
    val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
    pipeline(Post(url, body))
  }
  
  def delete(url: String) = {
    val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
    pipeline(Delete(url))
  }

  def waitforCompletion(queryId: String, retry: Int) = {

    val pipeline: HttpRequest => Future[Queries] = (
      addHeader("X-My-Special-Header", "fancy-value")
      ~> addCredentials(BasicHttpCredentials("bob", "secret"))
      ~> encode(Gzip)
      ~> sendReceive
      ~> decode(Deflate)
      ~> unmarshal[Queries])
    var offset = 0;
    var status = "IN_PROGRESS"
    var mutableRetry = retry

    while (status.equals("IN_PROGRESS") && mutableRetry != 0) {
      Thread.sleep(1000)
      val response: Future[Queries] = pipeline(Get(s"${jawsUrl}queries?queryID=$queryId"))
      mutableRetry = mutableRetry - 1
      Await.ready(response, Inf).value.get match {
        case Success(r: Queries) => {
          assert(r != null)
          status = r.queries(0).state
          println(s"Query status = $status, retry = $mutableRetry")
        }
        case Failure(e) => {
          println(e.getMessage)
          fail()
        }
      }
    }
    status
  }

  def getResults(queryId: String, offset: Long, limit: Int) = {
    val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
    var result: CustomResult = null
    val url = s"${jawsUrl}results?queryID=$queryId&offset=$offset&limit=$limit"
    val response: Future[HttpResponse] = pipeline(Get(url))
    val gson = new Gson()
    Await.ready(response, Inf).value.get match {
      case Success(r: HttpResponse) => {
        assert(r.status.isSuccess)
        val data = r.entity.data.asString
        result = gson.fromJson(data, classOf[CustomResult])
      }
      case Failure(e) => {
        println(e.getMessage)
        fail()
      }
    }
    result
  }

  def selectAllFromTable(url: String, tableName: String) = {
    val body = s"use $database;\nselect * from $tableName"

    val queryId = postRun(url, body)
    val queryStatus = waitforCompletion(queryId, 100)
    assert(queryStatus === "DONE", "Query is not DONE!")
    queryId
  }

  def validataAllResultsFromNormalTable(queryId: String, strings : Boolean) {

    val results = getResults(queryId, 0, 200)
    val flatResults = results.result.flatMap(x => x)
    assert(6 === results.result.length, "Different number of rows")
    assert(3 === results.result(0).length, "Different number of columns")
    assert(flatResults.containsSlice(GenSeq("Ana", if (strings == true) "5" else 5, "f")), "Ana is missing")
    assert(flatResults.containsSlice(GenSeq("George",  if (strings == true) "10" else 10, "m")), "George is missing")
    assert(flatResults.containsSlice(GenSeq("Alina",  if (strings == true) "20" else 20, "f")), "Alina is missing")
    assert(flatResults.containsSlice(GenSeq("Paul",  if (strings == true) "12" else 12, "m")), "Paul is missing")
    assert(flatResults.containsSlice(GenSeq("Pavel",  if (strings == true) "16" else 16, "m")), "Pavel is missing")
    assert(flatResults.containsSlice(GenSeq("Ioana",  if (strings == true) "30" else 30, "f")), "Ioana is missing")
  }

  def validataAllResultsFromParquetTable(queryId: String) {

    val results = getResults(queryId, 0, 200)
    val flatResults = results.result.flatMap(x => x)
    assert(9 === results.result.length, "Different number of rows")
    assert(1 === results.result(0).length, "Different number of columns")
    for (i <- 1 to 9) assert(flatResults.containsSlice(GenSeq(i)), s"Value $i does not exist")

  }
}