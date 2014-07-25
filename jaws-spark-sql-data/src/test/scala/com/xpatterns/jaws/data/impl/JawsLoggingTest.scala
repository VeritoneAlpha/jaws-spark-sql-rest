package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsLogging
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import com.xpatterns.jaws.data.utils.Randomizer
import com.typesafe.config.ConfigFactory
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.service.ThriftCluster
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy
import org.joda.time.DateTime
import com.xpatterns.jaws.data.utils.QueryState
import com.xpatterns.jaws.data.DTO.Log
import java.util.Collection

class JawsLoggingTest extends FunSuite with BeforeAndAfter {

  var logingDal: TJawsLogging = _

  before {
    if (logingDal == null) {

      val conf = ConfigFactory.load

      val cassandraConf = conf.getConfig("cassandraConf").withFallback(conf)

      // cassandra configuration
      val cassandraHost = cassandraConf.getString("cassandra.host")
      val cassandraKeyspace = cassandraConf.getString("cassandra.keyspace")
      val cassandraClusterName = cassandraConf.getString("cassandra.cluster.name")

      val cassandraHostConfigurator = new CassandraHostConfigurator(cassandraHost)
      val cluster = new ThriftCluster(cassandraClusterName, cassandraHostConfigurator)
      val keyspace = HFactory.createKeyspace(cassandraKeyspace, cluster, new AllOneConsistencyLevelPolicy)

      logingDal = new JawsCassandraLogging(keyspace)
    }

    logingDal
  }

  test("testWriteReadStatus") {
    val uuid = DateTime.now.toString();
    logingDal.setState(uuid, QueryState.IN_PROGRESS);
    val state1 = logingDal.getState(uuid);

    logingDal.setState(uuid, QueryState.DONE);
    val state2 = logingDal.getState(uuid);

    assert(QueryState.IN_PROGRESS === state1);
    assert(QueryState.DONE === state2);

  }

  test("testWriteReadMetaInfo") {
    val uuid = DateTime.now.toString()
    val metaInfo = Randomizer.createQueryMetainfo;
    logingDal.setMetaInfo(uuid, metaInfo)
    val result = logingDal.getMetaInfo(uuid);

    assert(metaInfo === result);
  }

  test("testWriteReadDetails") {
    val uuid = DateTime.now().toString();
    val details = Randomizer.getRandomString(10);

    logingDal.setScriptDetails(uuid, details);
    val resultDetails = logingDal.getScriptDetails(uuid);
    assert(details === resultDetails);
  }

  test("testWriteReadLogs") {
    val uuid = DateTime.now().toString();
    val queryId = Randomizer.getRandomString(5);
    val log = Randomizer.getRandomString(300);
    val now = System.currentTimeMillis();
    val logDto = new Log(log, queryId, now);

    logingDal.addLog(uuid, queryId, now, log);
    var result = logingDal.getLogs(uuid, now, 100);
    assert(1 === result.logs.size);
    assert(logDto === result.logs(0));

    logingDal.addLog(uuid, queryId, System.currentTimeMillis() + 100, log);
    logingDal.addLog(uuid, queryId, System.currentTimeMillis() + 200, log);
    logingDal.addLog(uuid, queryId, System.currentTimeMillis() + 300, log);

    result = logingDal.getLogs(uuid, now, 100);
    assert(4 === result.logs.size);

    result = logingDal.getLogs(uuid, now, 2);
    assert(2 === result.logs.size);
  }

  test("testWriteReadStates") {
    val uuid = DateTime.now() + " - 1";
    Thread.sleep(300);
    val uuid2 = DateTime.now() + " - 2";
    Thread.sleep(300);
    val uuid3 = DateTime.now() + " - 3";
    Thread.sleep(300);
    val uuid4 = DateTime.now() + " - 4";
    Thread.sleep(300);
    val uuid5 = DateTime.now() + " - 5";
    Thread.sleep(300);
    val uuid6 = DateTime.now() + " - 6";
    val queryId = Randomizer.getRandomString(5);
    val log = Randomizer.getRandomString(300);
    val now = System.currentTimeMillis();
    val logDto = new Log(log, queryId, now);

    logingDal.addLog(uuid, queryId, now, log);
    logingDal.addLog(uuid2, queryId, now, log);
    logingDal.addLog(uuid3, queryId, now, log);

    logingDal.setState(uuid, QueryState.DONE);
    logingDal.setState(uuid2, QueryState.IN_PROGRESS);
    logingDal.setState(uuid3, QueryState.FAILED);
    logingDal.setState(uuid4, QueryState.FAILED);
    logingDal.setState(uuid5, QueryState.FAILED);
    logingDal.setState(uuid6, QueryState.FAILED);

    var stateOfQuery = logingDal.getQueriesStates(null, 3);

    assert(3 === stateOfQuery.queries.size);

    assert(uuid6 === stateOfQuery.queries(0).queryID);
    assert(uuid5 === stateOfQuery.queries(1).queryID);
    assert(uuid4 === stateOfQuery.queries(2).queryID);

    stateOfQuery = logingDal.getQueriesStates(uuid4, 3);
    System.out.println(stateOfQuery);
    assert(3 === stateOfQuery.queries.size);

    assert(uuid3 === stateOfQuery.queries(0).queryID);
    assert(uuid2 === stateOfQuery.queries(1).queryID);
    assert(uuid === stateOfQuery.queries(2).queryID);

    stateOfQuery = logingDal.getQueriesStates(uuid3, 2);

    assert(2 === stateOfQuery.queries.size);

    assert(uuid2 === stateOfQuery.queries(0).queryID);
    assert(uuid === stateOfQuery.queries(1).queryID);

  }

}