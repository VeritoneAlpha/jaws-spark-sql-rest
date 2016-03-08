package api

import scala.concurrent._
import org.scalatest.FunSuite
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalamock.proxy.ProxyMockFactory
import org.scalatest.WordSpecLike
import org.scalatest.concurrent._
import server.JawsController
import com.xpatterns.jaws.data.contracts.DAL
import akka.actor.ActorRef
import server.Configuration
import com.xpatterns.jaws.data.impl.CassandraDal
import com.xpatterns.jaws.data.impl.HdfsDal
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
import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.utils.QueryState
import java.util.UUID
import apiactors.DeleteQueryApiActor
import messages.DeleteQueryMessage
import messages.ErrorMessage

@RunWith(classOf[JUnitRunner])
class DeleteQueryTest  extends FunSuite with BeforeAndAfter with ScalaFutures {

  val hdfsConf = JawsController.getHadoopConf
  var dals: DAL = _

  implicit val timeout = Timeout(10000)
  implicit val system = ActorSystem("localSystem")

  before {
    Configuration.dalType.getOrElse("cassandra") match {
      case "cassandra" => dals = new CassandraDal(Configuration.cassandraHost.get, Configuration.cassandraClusterName.get, Configuration.cassandraKeyspace.get)
      case _ => dals = new HdfsDal(hdfsConf)
    }
  }

  // **************** TESTS *********************

  test(" not found ") {

    val tAct = TestActorRef(new DeleteQueryApiActor(dals))
    val queryId = System.currentTimeMillis() + UUID.randomUUID().toString()
    val f = tAct ? DeleteQueryMessage(queryId)
    whenReady(f)(s => assert(s === new ErrorMessage(s"DELETE query failed with the following message: The query ${queryId} was not found. Please provide a valid query id")))

  }

  
  test(" in progress ") {

    val tAct = TestActorRef(new DeleteQueryApiActor(dals))
    val queryId = System.currentTimeMillis() + UUID.randomUUID().toString()
    dals.loggingDal.setState(queryId, QueryState.IN_PROGRESS)
    
    val f = tAct ? DeleteQueryMessage(queryId)
    whenReady(f)(s => assert(s === new ErrorMessage(s"DELETE query failed with the following message: The query ${queryId} is IN_PROGRESS. Please wait for its completion or cancel it")))
    
  }
  
  test(" ok ") {

    val tAct = TestActorRef(new DeleteQueryApiActor(dals))
    val queryId = System.currentTimeMillis() + UUID.randomUUID().toString()
    dals.loggingDal.setState(queryId, QueryState.DONE)
    
    val f = tAct ? DeleteQueryMessage(queryId)
    whenReady(f)(s => assert(s === s"Query ${queryId} was deleted"))
    
  }
}