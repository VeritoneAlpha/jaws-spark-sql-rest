package api

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.util.Timeout
import apiactors.QueryPropertiesApiActor
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import com.xpatterns.jaws.data.contracts.DAL
import com.xpatterns.jaws.data.impl.{HdfsDal, CassandraDal}
import com.xpatterns.jaws.data.utils.QueryState
import messages.{UpdateQueryPropertiesMessage, ErrorMessage}
import org.junit.runner.RunWith
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import server.{Configuration, JawsController}
import akka.pattern.ask
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Tests the query properties functionality
 */
@RunWith(classOf[JUnitRunner])
class QueryPropertiesTest extends FunSuite with BeforeAndAfter with ScalaFutures {
  val hdfsConf = JawsController.getHadoopConf()
  var dals: DAL = _

  private val timeout:PatienceConfiguration.Timeout = timeout(100.seconds)
  implicit val timeoutAsk = Timeout(100, TimeUnit.SECONDS)
  implicit val system = ActorSystem("localSystem")

  private var tAct:TestActorRef[QueryPropertiesApiActor] = null

  private val QUERY_NAME_1 = "query-name"
  private val QUERY_NAME_2 = "query-name-2"
  private val queryScript = "USE DATABASE test;"

  var queryId = ""

  before {
    Configuration.loggingType.getOrElse("cassandra") match {
      case "cassandra" => dals = new CassandraDal(Configuration.cassandraHost.get, Configuration.cassandraClusterName.get, Configuration.cassandraKeyspace.get)
      case _ => dals = new HdfsDal(hdfsConf)
    }
    tAct = TestActorRef(new QueryPropertiesApiActor(dals))
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
    val f = tAct ? UpdateQueryPropertiesMessage(queryId, Some(""), Some(""), None, overwrite = false)
    whenReady(f)(s => assert(s === new ErrorMessage(s"Updating query failed with the following message: " +
      s"The query $queryId was not found. Please provide a valid query id")))
  }

  test ("When setting the name the query should be updated") {
    val f = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_1), Some(""), None, overwrite = false)

    whenReady(f) (s => assert(s === s"Query information for $queryId has been updated"))
  }

  test ("When setting the name the query and searching for it, the query should be returned") {
    val f = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_1), Some(""), None, overwrite = false)

    whenReady(f, timeout) (s => {
      assert(s === s"Query information for $queryId has been updated")
      val query = dals.loggingDal.getQueriesByName(QUERY_NAME_1).queries(0)
      assert(query.query === queryScript)
      assert(query.queryID === queryId, "The query id is not the expected one")
      assert(query.metaInfo.name === Some(QUERY_NAME_1), "The query does not contain the expected name")
      assert(query.metaInfo.published === Some(false), "The query should not be published")
    })
  }

  test ("Updating the query name twice should delete the old name") {
    val f = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_1), Some(""), None, overwrite = false)
    Await.ready(f, 10.seconds)
    val f2 = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_2), Some(""), None, overwrite = false)
    whenReady(f2) (_ => {
      assert(dals.loggingDal.getQueriesByName(QUERY_NAME_1).queries.length === 0, "The first query name should not be found")
      val query = dals.loggingDal.getQueriesByName(QUERY_NAME_2).queries(0)
      assert(query.query === queryScript)
      assert(query.queryID === queryId, "The query id is not the expected one")
      assert(query.metaInfo.name === Some(QUERY_NAME_2), "The query does not contain the expected name")
    })
  }

  test ("Setting a name for the queries that is not unique should throw exception when overwrite is false") {
    val secondQueryId = System.currentTimeMillis() + UUID.randomUUID().toString
    createQuery(secondQueryId, queryScript)

    val f = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_1), Some(""), None, overwrite = false)
    Await.ready(f, 10.seconds)
    val f2 = tAct ? UpdateQueryPropertiesMessage(secondQueryId, Some(QUERY_NAME_1), Some(""), None, overwrite = false)

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

    val f = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_1), Some(""), None, overwrite = false)
    Await.ready(f, 10.seconds)
    val f2 = tAct ? UpdateQueryPropertiesMessage(secondQueryId, Some(QUERY_NAME_1), Some(""), None, overwrite = true)

    whenReady(f2) (s => {
      assert(s === s"Query information for $secondQueryId has been updated")
      val query = dals.loggingDal.getQueriesByName(QUERY_NAME_1).queries(0)
      assert(query.query === queryScript)
      assert(query.queryID === secondQueryId)
    })
  }

  test ("Setting a query that has a name with null should delete its name") {
    val f = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_1), Some(""), None, overwrite = false)
    Await.ready(f, 10.seconds)
    val f2 = tAct ? UpdateQueryPropertiesMessage(queryId, Some(null), Some(""), None, overwrite = true)

    whenReady(f2) (s => {
      val newMetaInfo = dals.loggingDal.getMetaInfo(queryId)
      assert(newMetaInfo.name === None)
      assert(dals.loggingDal.getQueriesByName(QUERY_NAME_1).queries.length === 0, "The name of the query should be deleted")
      assert(s === s"Query information for $queryId has been updated")
    })
  }

  test ("Setting a query published should keep the flag") {
    val f = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_1), Some(""), Some(true), overwrite = false)
    whenReady(f, timeout) (s => {
      val newMetaInfo = dals.loggingDal.getMetaInfo(queryId)
      assert(newMetaInfo.published === Some(true), "The flag is not saved")

      val publishedQueries = dals.loggingDal.getPublishedQueries()
      assert(publishedQueries.length === 1, "There should be one query published")
      assert(publishedQueries(0) === QUERY_NAME_1, "The published query name is not found")
    })
  }

  test ("Renaming a published query should keep the flag") {
    val f = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_1), Some(""), Some(true), overwrite = false)
    Await.ready(f, 10.seconds)
    val f2 = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_2), Some(""), None, overwrite = true)

    whenReady(f2) (s => {
      val newMetaInfo = dals.loggingDal.getMetaInfo(queryId)
      assert(newMetaInfo.published === Some(true), "The flag is not saved")

      val publishedQueries = dals.loggingDal.getPublishedQueries()
      assert(publishedQueries.length === 1, "There should be one query published")
      assert(publishedQueries(0) === QUERY_NAME_2, "The published query name is not found")
    })
  }

  test ("Deleting the name of a published query should remove the flag") {
    val f = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_1), Some(""), Some(true), overwrite = false)
    Await.ready(f, 10.seconds)
    val f2 = tAct ? UpdateQueryPropertiesMessage(queryId, Some(null), Some(""), None, overwrite = true)

    whenReady(f2) (s => {
      val newMetaInfo = dals.loggingDal.getMetaInfo(queryId)
      assert(newMetaInfo.published === None, "The flag should not be saved")
      val publishedQueries = dals.loggingDal.getPublishedQueries()
      assert(publishedQueries.length === 0, "There should be no query published")
    })
  }

  test ("Unpublishing a query should set the flag on false") {
    val f = tAct ? UpdateQueryPropertiesMessage(queryId, Some(QUERY_NAME_1), Some(""), Some(true), overwrite = false)
    Await.ready(f, 10.seconds)
    val f2 = tAct ? UpdateQueryPropertiesMessage(queryId, None, None, Some(false), overwrite = true)

    whenReady(f2) (s => {
      val newMetaInfo = dals.loggingDal.getMetaInfo(queryId)
      assert(newMetaInfo.published === Some(false), "The flag should be on false")
    })
  }
}
