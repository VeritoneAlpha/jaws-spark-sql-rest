package com.xpatterns.jaws.data.impl

import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.service.ThriftCluster
import me.prettyprint.hector.api.factory.HFactory
import com.xpatterns.jaws.data.contracts.DAL
import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.contracts.TJawsResults
import com.xpatterns.jaws.data.contracts.TJawsParquetTables

/**
 * Created by emaorhian
 */
class CassandraDal (cassandraHost : String, clusterName : String, keyspaceName : String) extends DAL {
  val cassandraHostConfigurator = new CassandraHostConfigurator(cassandraHost)
  val cluster = new ThriftCluster(clusterName, cassandraHostConfigurator)
  val keyspace = HFactory.createKeyspace(keyspaceName, cluster, new AllOneConsistencyLevelPolicy)

  val loggingDal: TJawsLogging = new JawsCassandraLogging(keyspace)
  val resultsDal: TJawsResults = new JawsCassandraResults(keyspace)
  val parquetTableDal: TJawsParquetTables = new JawsCassandraParquetTables(keyspace)
}