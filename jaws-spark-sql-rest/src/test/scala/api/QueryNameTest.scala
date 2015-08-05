package api

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.util.Timeout
import apiactors.QueryNameApiActor
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import com.xpatterns.jaws.data.contracts.DAL
import com.xpatterns.jaws.data.impl.{HdfsDal, CassandraDal}
import com.xpatterns.jaws.data.utils.QueryState
import messages.{UpdateQueryNameMessage, ErrorMessage}
import org.junit.runner.RunWith
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import server.{Configuration, JawsController}
import akka.pattern.ask
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Tests the query name functionality
 */
@RunWith(classOf[JUnitRunner])
class QueryNameTest extends FunSuite with BeforeAndAfter with ScalaFutures {
  val hdfsConf = JawsController.getHadoopConf()
  var dals: DAL = _

  implicit val timeout = Timeout(100, TimeUnit.SECONDS)
  implicit val system = ActorSystem("localSystem")

  private var tAct:TestActorRef[QueryNameApiActor] = null

  private val QUERY_NAME_1 = "query-name"
  private val QUERY_NAME_2 = "query-name-2"
  private val queryScript = "USE DATABASE test;"

  var queryId = ""



  before {
    Configuration.loggingType.getOrElse("cassandra") match {
      case "cassandra" => dals = new CassandraDal(Configuration.cassandraHost.get, Configuration.cassandraClusterName.get, Configuration.cassandraKeyspace.get)
      case _ => dals = new HdfsDal(hdfsConf)
    }
    tAct = TestActorRef(new QueryNameApiActor(dals))
    queryId = System.currentTimeMillis() + UUID.randomUUID().toString
    createQuery(queryId, queryScript)
  }

  after {
    dals.loggingDal.deleteQuery(queryId)
    dals.loggingDal.deleteQueryName(QUERY_NAME_1)
    dals.loggingDal.deleteQueryName(QUERY_NAME_2)
  }

  def createQuery(queryId:String, queryScript:String): Unit = {
    val metaInfo = new QueryMetaInfo()
    dals.loggingDal.setMetaInfo(queryId, metaInfo)
    dals.loggingDal.setState(queryId, QueryState.IN_PROGRESS)
    dals.loggingDal.setScriptDetails(queryId, queryScript)
  }

  // **************** TESTS *********************

  test(" not found ") {
    val queryId = System.currentTimeMillis() + UUID.randomUUID().toString
    val f = tAct ? UpdateQueryNameMessage(queryId, "", "", overwrite = false)
    whenReady(f)(s => assert(s === new ErrorMessage(s"Updating query failed with the following message: " +
      s"The query $queryId was not found. Please provide a valid query id")))
  }

  test ("When setting the name the query should be updated") {
    val f = tAct ? UpdateQueryNameMessage(queryId, QUERY_NAME_1, "", overwrite = false)

    whenReady(f) (s => assert(s === s"Query information for $queryId has been updated"))
  }

  test ("When setting the name the query and searching for it, the query should be returned") {
    val f = tAct ? UpdateQueryNameMessage(queryId, QUERY_NAME_1, "", overwrite = false)

    whenReady(f) (s => {
      assert(s === s"Query information for $queryId has been updated")
      val query = dals.loggingDal.getQueriesByName(QUERY_NAME_1).queries(0)
      assert(query.query === queryScript)
      assert(query.queryID === queryId, "The query id is not the expected one")
      assert(query.metaInfo.name === QUERY_NAME_1, "The query does not contain the expected name")
    })
  }

  test ("Updating the query name twice should delete the old name") {
    val f = tAct ? UpdateQueryNameMessage(queryId, QUERY_NAME_1, "", overwrite = false)
    Await.ready(f, 10.seconds)
    val f2 = tAct ? UpdateQueryNameMessage(queryId, QUERY_NAME_2, "", overwrite = false)
    whenReady(f2) (_ => {
      assert(dals.loggingDal.getQueriesByName(QUERY_NAME_1).queries.length === 0, "The first query name should not be found")
      val query = dals.loggingDal.getQueriesByName(QUERY_NAME_2).queries(0)
      assert(query.query === queryScript)
      assert(query.queryID === queryId, "The query id is not the expected one")
      assert(query.metaInfo.name === QUERY_NAME_2, "The query does not contain the expected name")
    })
  }

  test ("Setting a name for the queries that is not unique should throw exception when overwrite is false") {
    val secondQueryId = System.currentTimeMillis() + UUID.randomUUID().toString
    createQuery(secondQueryId, queryScript)

    val f = tAct ? UpdateQueryNameMessage(queryId, QUERY_NAME_1, "", overwrite = false)
    Await.ready(f, 10.seconds)
    val f2 = tAct ? UpdateQueryNameMessage(secondQueryId, QUERY_NAME_1, "", overwrite = false)

    whenReady(f2) (s => {
      assert(s === ErrorMessage(s"Updating query failed with the following message: There is already a query with the name $QUERY_NAME_1. " +
        s"To overwrite the query name, please send the parameter overwrite set on true"))
      val query = dals.loggingDal.getQueriesByName(QUERY_NAME_1).queries(0)
      assert(query.query === queryScript)
      assert(query.queryID === queryId)
    })
  }

  test ("Setting a name for the queries that is not unique should be updated when overwrite is true") {
    val secondQueryId = System.currentTimeMillis() + UUID.randomUUID().toString
    createQuery(secondQueryId, queryScript)

    val f = tAct ? UpdateQueryNameMessage(queryId, QUERY_NAME_1, "", overwrite = false)
    Await.ready(f, 10.seconds)
    val f2 = tAct ? UpdateQueryNameMessage(secondQueryId, QUERY_NAME_1, "", overwrite = true)

    whenReady(f2) (s => {
      assert(s === s"Query information for $secondQueryId has been updated")
      val query = dals.loggingDal.getQueriesByName(QUERY_NAME_1).queries(0)
      assert(query.query === queryScript)
      assert(query.queryID === secondQueryId)
    })
  }
}
