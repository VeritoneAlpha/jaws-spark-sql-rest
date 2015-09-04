package api

import com.google.gson.Gson
import com.xpatterns.jaws.data.DTO.{Tables, Databases}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import spray.client.pipelining._
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration._
import scala.util.Success
import scala.util.Failure
import spray.http._
import spray.httpx.SprayJsonSupport._
import foundation.TestBase
import scala.concurrent._
import ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class GetDatabasesApiTest extends TestBase {

  test(" get databases ") {
    val url = s"${jawsUrl}hive/databases"

    val pipeline: HttpRequest => Future[Databases] = (
      addHeader("X-My-Special-Header", "fancy-value")
      ~> sendReceive
      ~> unmarshal[Databases])

    val response: Future[Databases] = pipeline(Get(url))
    Await.ready(response, Inf).value.get match {
      case Success(r: Databases) =>
        assert(r != null)
        assert(r.databases.contains("default"))

      case Failure(e) =>
        println(e.getMessage)
        fail()
    }
  }

  test("tables api") {
    val response = get(s"${jawsUrl}hive/tables")

    Await.ready(response, Inf).value.get match {
      case Success(r: HttpResponse) =>
        assert(r.status.isSuccess)
        val responseText = r.entity.data.asString
        val gson = new Gson()
        val tables = gson.fromJson(responseText, classOf[Array[Tables]])
        assert(tables.nonEmpty, "There is no table")

      case Failure(e) =>
        println(e.getMessage)
        fail()
    }
  }
}