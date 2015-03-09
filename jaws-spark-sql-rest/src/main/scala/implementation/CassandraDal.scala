package implementation

import com.xpatterns.jaws.data.impl.JawsCassandraLogging
import com.xpatterns.jaws.data.impl.JawsCassandraResults
import server.Configuration
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.service.ThriftCluster
import me.prettyprint.hector.api.factory.HFactory
import traits.DAL
import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.contracts.TJawsResults
import com.xpatterns.jaws.data.contracts.TJawsParquetTables
import com.xpatterns.jaws.data.impl.JawsCassandraParquetTables

/**
 * Created by emaorhian
 */
class CassandraDal extends DAL {
  val cassandraHostConfigurator = new CassandraHostConfigurator(Configuration.cassandraHost.get)
  val cluster = new ThriftCluster(Configuration.cassandraClusterName.get, cassandraHostConfigurator)
  val keyspace = HFactory.createKeyspace(Configuration.cassandraKeyspace.get, cluster, new AllOneConsistencyLevelPolicy)

  val loggingDal: TJawsLogging = new JawsCassandraLogging(keyspace)
  val resultsDal: TJawsResults = new JawsCassandraResults(keyspace)
  val parquetTableDal: TJawsParquetTables = new JawsCassandraParquetTables(keyspace)
}