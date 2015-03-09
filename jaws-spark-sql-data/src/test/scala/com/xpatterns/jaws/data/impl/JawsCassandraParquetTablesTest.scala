package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsParquetTables
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import com.typesafe.config.ConfigFactory
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.service.ThriftCluster
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy
import com.xpatterns.jaws.data.DTO.ParquetTable
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.xpatterns.jaws.data.utils.Randomizer

@RunWith(classOf[JUnitRunner])
class JawsCassandraParquetTablesTest extends FunSuite with BeforeAndAfter {

  var pTablesDal: TJawsParquetTables = _

  before {
    if (pTablesDal == null) {

      val conf = ConfigFactory.load

      val cassandraConf = conf.getConfig("cassandraConf").withFallback(conf)

      // cassandra configuration
      val cassandraHost = cassandraConf.getString("cassandra.host")
      val cassandraKeyspace = cassandraConf.getString("cassandra.keyspace")
      val cassandraClusterName = cassandraConf.getString("cassandra.cluster.name")

      val cassandraHostConfigurator = new CassandraHostConfigurator(cassandraHost)
      val cluster = new ThriftCluster(cassandraClusterName, cassandraHostConfigurator)
      val keyspace = HFactory.createKeyspace(cassandraKeyspace, cluster, new AllOneConsistencyLevelPolicy)
      
      //!!!!!!! ATTENTION !!!! truncating CF
      cluster.truncate(keyspace.getKeyspaceName(), "parquet_tables")

      pTablesDal = new JawsCassandraParquetTables(keyspace)
    }

    pTablesDal
  }

  test("testAddReadTable") {
    val table = Randomizer.getParquetTable

    pTablesDal.addParquetTable(table)
    val resultTable = pTablesDal.readParquetTable(table.name)
    assert(table === resultTable)
    pTablesDal.deleteParquetTable(table.name)

  }

  test("testDeleteTable") {
    val table = Randomizer.getParquetTable

    pTablesDal.addParquetTable(table)
    val tableBeforeDeletion = pTablesDal.readParquetTable(table.name)
    pTablesDal.deleteParquetTable(table.name)
    val tableAfterDeletion = pTablesDal.readParquetTable(table.name)

    assert(table === tableBeforeDeletion)
    assert(new ParquetTable === tableAfterDeletion)

  }

  test("testDeleteUnexistingTable") {
    val tName = Randomizer.getRandomString(5)
    pTablesDal.deleteParquetTable(tName)
    val tableAfterDeletion = pTablesDal.readParquetTable(tName)

    assert(new ParquetTable === tableAfterDeletion)

  }

  test("testTableDoesntExist") {
    val tName = Randomizer.getRandomString(5)
    assert(false === pTablesDal.tableExists(tName))
  }

  test("testTableExists") {
    val table = Randomizer.getParquetTable
    pTablesDal.addParquetTable(table)
    assert(true === pTablesDal.tableExists(table.name))
    pTablesDal.deleteParquetTable(table.name)
  }

  test("testGetTables Empty") {
    val result = pTablesDal.listParquetTables
    assert(false === (result == null))
    assert(0 === result.size)
  }
  
  test("testGetTables") {
    val tables = Randomizer.getParquetTables(5)
    tables.foreach(table => pTablesDal.addParquetTable(table))
    val result = pTablesDal.listParquetTables
    tables.foreach(table => pTablesDal.deleteParquetTable(table.name))

    assert(false === (result == null))
    assert(5 === result.size)
    tables.foreach(table => assert(true === result.contains(table)))
  }
}