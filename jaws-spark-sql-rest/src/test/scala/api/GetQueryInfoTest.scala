package api

import scala.concurrent._
import org.scalatest.FunSuite
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalamock.proxy.ProxyMockFactory
import org.scalatest.WordSpecLike
import org.scalatest.concurrent._
import server.JawsController
import traits.DAL
import akka.actor.ActorRef
import server.Configuration
import implementation.CassandraDal
import implementation.HdfsDal
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorSystem
import akka.actor.Props
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.util.Timeout
import akka.pattern.ask
import com.xpatterns.jaws.data.DTO.Query
import scala.concurrent.duration._
import akka.testkit.TestActorRef
import akka.actor.Status.Success
import apiactors.GetQueryInfoApiActor
import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.utils.QueryState
import messages.GetQueryInfoMessage
import java.util.UUID

@RunWith(classOf[JUnitRunner])
class GetQueryInfoTest extends FunSuite with BeforeAndAfter with ScalaFutures {

  val hdfsConf = JawsController.getHadoopConf
  var dals: DAL = _

  implicit val timeout = Timeout(10000)
  implicit val system = ActorSystem("localSystem")

  before {
    Configuration.loggingType.getOrElse("cassandra") match {
      case "cassandra" => dals = new CassandraDal()
      case _ => dals = new HdfsDal(hdfsConf)
    }
  }

  // **************** TESTS *********************

  test(" not found ") {

    val tAct = TestActorRef(new GetQueryInfoApiActor(dals))
    val queryId = System.currentTimeMillis() + UUID.randomUUID().toString()
    val f = tAct ? GetQueryInfoMessage(queryId)
    whenReady(f)(s => assert(s === new Query("NOT_FOUND", queryId, "")))

  }

  
  test(" found ") {

    val tAct = TestActorRef(new GetQueryInfoApiActor(dals))
    val queryId = System.currentTimeMillis() + UUID.randomUUID().toString()
    dals.loggingDal.setState(queryId, QueryState.IN_PROGRESS)
    dals.loggingDal.setScriptDetails(queryId, "test script")
    
    val f = tAct ? GetQueryInfoMessage(queryId)
    whenReady(f)(s => assert(s === new Query("IN_PROGRESS", queryId, "test script")))
    
    dals.loggingDal.deleteQuery(queryId)

  }
}