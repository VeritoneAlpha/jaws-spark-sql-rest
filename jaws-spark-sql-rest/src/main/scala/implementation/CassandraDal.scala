package implementation

import com.xpatterns.jaws.data.contracts.IJawsLogging
import com.xpatterns.jaws.data.contracts.IJawsResults
import com.xpatterns.jaws.data.impl.JawsCassandraLogging
import com.xpatterns.jaws.data.impl.JawsCassandraResults

import actors.Configuration
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.service.ThriftCluster
import me.prettyprint.hector.api.factory.HFactory
import traits.DAL

/**
 * Created by emaorhian
 */
class CassandraDal extends DAL {
  val cassandraHostConfigurator = new CassandraHostConfigurator(Configuration.cassandraHost.get)
  val cluster = new ThriftCluster(Configuration.cassandraClusterName.get, cassandraHostConfigurator)
  val keyspace = HFactory.createKeyspace(Configuration.cassandraKeyspace.get, cluster, new AllOneConsistencyLevelPolicy)

  val loggingDal: IJawsLogging = new JawsCassandraLogging(keyspace)
  val resultsDal: IJawsResults = new JawsCassandraResults(keyspace)

}