package com.xpatterns.jaws.data.impl

import org.scalatest.BeforeAndAfter
import com.xpatterns.jaws.data.contracts.TJawsLogging
import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import com.xpatterns.jaws.data.utils.QueryState
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import com.xpatterns.jaws.data.utils.Utils
import org.apache.hadoop.conf.Configuration
import com.xpatterns.jaws.data.utils.Randomizer
import com.xpatterns.jaws.data.DTO.Log
import com.xpatterns.jaws.data.utils.QueryState
import com.xpatterns.jaws.data.utils.Randomizer

class JawsLoggingOnHdfsTest extends FunSuite with BeforeAndAfter {

  var logingDal: TJawsLogging = _

  before {
    if (logingDal == null) {

      val conf = ConfigFactory.load

      val hadoopConf = conf.getConfig("hadoopConf").withFallback(conf)

      //hadoop conf 
      val replicationFactor = Option(hadoopConf.getString("replicationFactor"))
      val forcedMode = Option(hadoopConf.getString("forcedMode"))
      val loggingFolder = Option(hadoopConf.getString("loggingFolder"))
      val stateFolder = Option(hadoopConf.getString("stateFolder"))
      val detailsFolder = Option(hadoopConf.getString("detailsFolder"))
      val resultsFolder = Option(hadoopConf.getString("resultsFolder"))
      val metaInfoFolder = Option(hadoopConf.getString("metaInfoFolder"))
      val namenode = Option(hadoopConf.getString("namenode"))

      val configuration = new org.apache.hadoop.conf.Configuration()
      configuration.setBoolean(Utils.FORCED_MODE, forcedMode.getOrElse("false").toBoolean)

      // set hadoop name node and job tracker
      namenode match {
        case None => {
          throw new RuntimeException("You need to set the namenode! ")
        }
        case _ => configuration.set("fs.defaultFS", namenode.get)

      }

      configuration.set("dfs.replication", replicationFactor.getOrElse("1"))

      configuration.set(Utils.LOGGING_FOLDER, loggingFolder.getOrElse("jawsLogs"));
      configuration.set(Utils.STATUS_FOLDER, stateFolder.getOrElse("jawsStates"));
      configuration.set(Utils.DETAILS_FOLDER, detailsFolder.getOrElse("jawsDetails"));
      configuration.set(Utils.METAINFO_FOLDER, metaInfoFolder.getOrElse("jawsMetainfoFolder"));
      configuration.set(Utils.RESULTS_FOLDER, resultsFolder.getOrElse("jawsResultsFolder"));
      logingDal = new JawsHdfsLogging(configuration)
    }

    logingDal
  }

   test("testWriteReadStatus") {
    val uuid = DateTime.now.getMillis().toString;
    logingDal.setState(uuid, QueryState.IN_PROGRESS);
    val state1 = logingDal.getState(uuid);

    logingDal.setState(uuid, QueryState.DONE);
    val state2 = logingDal.getState(uuid);

    assert(QueryState.IN_PROGRESS === state1);
    assert(QueryState.DONE === state2);

  }

  test("testWriteReadMetaInfo") {
    val uuid = DateTime.now.getMillis().toString
    val metaInfo = Randomizer.createQueryMetainfo;
    logingDal.setMetaInfo(uuid, metaInfo)
    val result = logingDal.getMetaInfo(uuid);

    assert(metaInfo === result);
  }

  test("testWriteReadDetails") {
    val uuid = DateTime.now.getMillis().toString
    val details = Randomizer.getRandomString(10);

    logingDal.setScriptDetails(uuid, details);
    val resultDetails = logingDal.getScriptDetails(uuid);
    assert(details === resultDetails);
  }

  test("testWriteReadLogs") {
    val uuid =DateTime.now.getMillis().toString
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
    val uuid = DateTime.now.getMillis().toString + " - 1";
    Thread.sleep(300);
    val uuid2 = DateTime.now.getMillis().toString + " - 2";
    Thread.sleep(300);
    val uuid3 =DateTime.now.getMillis().toString + " - 3";
    Thread.sleep(300);
    val uuid4 = DateTime.now.getMillis().toString + " - 4";
    Thread.sleep(300);
    val uuid5 = DateTime.now.getMillis().toString + " - 5";
    Thread.sleep(300);
    val uuid6 = DateTime.now.getMillis().toString + " - 6";
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