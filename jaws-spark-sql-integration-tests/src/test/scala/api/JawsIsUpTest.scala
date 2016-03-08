package api

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import foundation.TestBase
import spray.http._
import spray.client.pipelining._
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration._
import scala.util.Success
import scala.util.Failure

@RunWith(classOf[JUnitRunner])
class JawsIsUpTest extends TestBase {
  
  test(" Jaws is up and running ") {
    implicit val system = ActorSystem()
    import system.dispatcher // execution context for futures

    val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
    val response: Future[HttpResponse] = pipeline(Get(s"${jawsUrl}index"))
    
    Await.ready(response, Inf).value.get  match {
      case Success(r : HttpResponse) =>
        assert(r.status.isSuccess)
        assert(r.entity.data.asString === "Jaws is up and running!", "Jaws is not Up!")

      case Failure(e) =>
        println(e.getMessage)
        fail()

    }
  }
  
}