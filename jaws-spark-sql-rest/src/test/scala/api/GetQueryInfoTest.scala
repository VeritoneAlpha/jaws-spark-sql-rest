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
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import apiactors.GetQueriesApiActor
import messages.GetQueriesMessage
import com.xpatterns.jaws.data.DTO.Queries

@RunWith(classOf[JUnitRunner])
class GetQueryInfoTest extends FunSuite with BeforeAndAfter with ScalaFutures {

  val hdfsConf = JawsController.getHadoopConf
  var dals: DAL = _

  implicit val timeout = Timeout(10000)
  implicit val system = ActorSystem("localSystem")

  before {
    Configuration.loggingType.getOrElse("cassandra") match {
      case "cassandra" => dals = new CassandraDal(Configuration.cassandraHost.get, Configuration.cassandraClusterName.get, Configuration.cassandraKeyspace.get)
      case _ => dals = new HdfsDal(hdfsConf)
    }
  }

  // **************** TESTS *********************

  test(" not found ") {

    val tAct = TestActorRef(new GetQueriesApiActor(dals))
    val queryId = System.currentTimeMillis() + UUID.randomUUID().toString()
    val f = tAct ? GetQueriesMessage(Seq(queryId))
    whenReady(f)(s => s match {
      case queries: Queries => {
    	assert(queries.queries.size === 1)
        assert(queries.queries(0) === new Query("NOT_FOUND", queryId, "", new QueryMetaInfo))
      }
      case _ => fail
    })
  }

  test(" found ") {

    val tAct = TestActorRef(new GetQueriesApiActor(dals))
    val queryId = System.currentTimeMillis() + UUID.randomUUID().toString()
    val metaInfo = new QueryMetaInfo(100, 150, 1, true)
    dals.loggingDal.setState(queryId, QueryState.IN_PROGRESS)
    dals.loggingDal.setScriptDetails(queryId, "test script")
    dals.loggingDal.setMetaInfo(queryId, metaInfo)

    val f = tAct ? GetQueriesMessage(Seq(queryId))
    whenReady(f)(s => s match {
      case queries: Queries => {
    	assert(queries.queries.size === 1)
        assert(queries.queries(0) === new Query("IN_PROGRESS", queryId, "test script", metaInfo))
      }
      case _ => fail
    })
    
    dals.loggingDal.deleteQuery(queryId)

  }
}