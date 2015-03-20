package foundation

import com.xpatterns.jaws.data.DTO.Result

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
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
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import com.typesafe.config.ConfigFactory
import com.xpatterns.jaws.data.DTO.Logs
import spray.httpx.encoding.Deflate

import spray.http._
import spray.httpx.encoding.{ Gzip, Deflate }
import spray.httpx.SprayJsonSupport._


class TestBase extends FunSuite with BeforeAndAfterAll {
  implicit val system = ActorSystem()
  import system.dispatcher // execution context for futures

  private val conf = ConfigFactory.load
  conf.checkValid(ConfigFactory.defaultReference)

  val appConf = conf.getConfig("appConf")
  val jawsUrl = appConf.getString("jawsUrl")
  val namenodeIp = appConf.getString("namenodeIp")
  val hdfsInputFolder = appConf.getString("hdfsInputFolder")
  val database = appConf.getString("database")
  val table = appConf.getString("table")
  val runTachyon = appConf.getBoolean("runTachyon")

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

  def waitforCompletion(queryId: String, retry: Int) = {

    val pipeline: HttpRequest => Future[Logs] = (
      addHeader("X-My-Special-Header", "fancy-value")
      ~> addCredentials(BasicHttpCredentials("bob", "secret"))
      ~> encode(Gzip)
      ~> sendReceive
      ~> decode(Deflate)
      ~> unmarshal[Logs])
    var offset = 0;
    var status = "IN_PROGRESS"
    var mutableRetry = retry

    while (status.equals("IN_PROGRESS") && mutableRetry != 0) {
      Thread.sleep(1000)
      val response: Future[Logs] = pipeline(Get(s"${jawsUrl}logs?queryID=$queryId&offset=$offset&limit=100"))
      mutableRetry = mutableRetry - 1
      Await.ready(response, Inf).value.get match {
        case Success(r: Logs) => {
          assert(r != null)
          status = r.status
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
  
  def getResults(queryId : String, offset:Long, limit : Int) = {
    val pipeline: HttpRequest => Future[Result] = (
      addHeader("X-My-Special-Header", "fancy-value")
      ~> addCredentials(BasicHttpCredentials("bob", "secret"))
      ~> encode(Gzip)
      ~> sendReceive
      ~> decode(Deflate)
      ~> unmarshal[Result])
    var result : Result = null  
    val url = s"${jawsUrl}results?queryID=$queryId&offset=$offset&limit=$limit"  
    val response: Future[Result] = pipeline(Get(url))
    Await.ready(response, Inf).value.get match {
      case Success(r: Result) => {
        assert(r != null)
        result = r
      }
      case Failure(e) => {
        println(e.getMessage)
        fail()
      }
    }
    result
  }
}