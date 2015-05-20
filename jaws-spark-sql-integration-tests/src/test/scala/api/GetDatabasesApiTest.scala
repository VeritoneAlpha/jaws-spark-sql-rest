package api

import com.xpatterns.jaws.data.DTO.Databases
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
import scala.collection.GenSeq
import foundation.TestBase
import scala.concurrent._
import ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class GetDatabasesApiTest extends TestBase {

  test(" get databases ") {

    val username = System.getProperties().get("user.name")
    val url = s"${jawsUrl}databases"

    val pipeline: HttpRequest => Future[Databases] = (
      addHeader("X-My-Special-Header", "fancy-value")
      ~> addCredentials(BasicHttpCredentials("bob", "secret"))
      ~> encode(Gzip)
      ~> sendReceive
      ~> decode(Deflate)
      ~> unmarshal[Databases])

    val response: Future[Databases] = pipeline(Get(url))
    Await.ready(response, Inf).value.get match {
      case Success(r: Databases) => {
        assert(r != null)
        assert(r.databases.contains("default"))
      }
      case Failure(e) => {
        println(e.getMessage)
        fail()
      }
    }
  }
}