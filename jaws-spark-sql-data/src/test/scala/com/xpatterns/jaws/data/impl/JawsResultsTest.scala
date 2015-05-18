package com.xpatterns.jaws.data.impl

import org.scalatest.FunSuite
import com.xpatterns.jaws.data.DTO.Column
import org.apache.commons.lang.RandomStringUtils
import com.xpatterns.jaws.data.utils.Randomizer
import com.xpatterns.jaws.data.contracts.TJawsResults
import org.scalatest.BeforeAndAfter
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import org.junit.Assert
import com.typesafe.config.ConfigFactory
import me.prettyprint.cassandra.service.ThriftCluster
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.xpatterns.jaws.data.DTO.AvroResult
import com.xpatterns.jaws.data.DTO.CustomResult

@RunWith(classOf[JUnitRunner])
class JawsResultsTest extends FunSuite with BeforeAndAfter {

  var resultsDal: TJawsResults = _

  before {
    if (resultsDal == null) {

      val conf = ConfigFactory.load

      val cassandraConf = conf.getConfig("cassandraConf").withFallback(conf)

      // cassandra configuration
      val cassandraHost = cassandraConf.getString("cassandra.host")
      val cassandraKeyspace = cassandraConf.getString("cassandra.keyspace")
      val cassandraClusterName = cassandraConf.getString("cassandra.cluster.name")

      val cassandraHostConfigurator = new CassandraHostConfigurator(cassandraHost)
      val cluster = new ThriftCluster(cassandraClusterName, cassandraHostConfigurator)
      val keyspace = HFactory.createKeyspace(cassandraKeyspace, cluster, new AllOneConsistencyLevelPolicy)

      resultsDal = new JawsCassandraResults(keyspace)
    }

    resultsDal
  }

  test("testWriteReadResults") {
    val uuid = Randomizer.getRandomString(10)
    val resultsConverter = Randomizer.getResultsConverter
    resultsDal.setResults(uuid, resultsConverter)

    val avroResults = resultsDal.getAvroResults(uuid)
    val customResults = resultsDal.getCustomResults(uuid)

    assert(resultsConverter.toAvroResults() === avroResults)
    assert(resultsConverter.toCustomResults() === customResults)

  }

  test("testDeleteResults") {
    val uuid = Randomizer.getRandomString(10)
    val resultsConverter = Randomizer.getResultsConverter
    resultsDal.setResults(uuid, resultsConverter)

    val avroResults = resultsDal.getAvroResults(uuid)
    val customResults = resultsDal.getCustomResults(uuid)

    resultsDal.deleteResults(uuid)

    val avroResultsDeleted = resultsDal.getAvroResults(uuid)
    val customResultsDeleted = resultsDal.getCustomResults(uuid)

    assert(resultsConverter.toAvroResults() === avroResults)
    assert(resultsConverter.toCustomResults() === customResults)
    assert(new AvroResult() === avroResultsDeleted)
     assert(new CustomResult() === customResultsDeleted)

  }
}