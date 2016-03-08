package server

import com.typesafe.config.Config
import org.apache.log4j.Logger

/**
 * Holds the configuration properties for Jaws
 */
object Configuration {

  import com.typesafe.config.ConfigFactory

  val log4j = Logger.getLogger(Configuration.getClass)

  private val conf = ConfigFactory.load
  conf.checkValid(ConfigFactory.defaultReference)

  val remote = conf.getConfig("remote")
  val sparkConf = conf.getConfig("sparkConfiguration")
  val appConf = conf.getConfig("appConf")
  val hadoopConf = conf.getConfig("hadoopConf")
  val cassandraConf = conf.getConfig("cassandraConf")

  // cassandra configuration
  val cassandraHost = getStringConfiguration(cassandraConf, "cassandra.host")
  val cassandraKeyspace = getStringConfiguration(cassandraConf, "cassandra.keyspace")
  val cassandraClusterName = getStringConfiguration(cassandraConf, "cassandra.cluster.name")

  //hadoop conf
  val replicationFactor = getStringConfiguration(hadoopConf, "replicationFactor")
  val forcedMode = getStringConfiguration(hadoopConf, "forcedMode")
  val loggingFolder = getStringConfiguration(hadoopConf, "loggingFolder")
  val stateFolder = getStringConfiguration(hadoopConf, "stateFolder")
  val detailsFolder = getStringConfiguration(hadoopConf, "detailsFolder")
  val resultsFolder = getStringConfiguration(hadoopConf, "resultsFolder")
  val metaInfoFolder = getStringConfiguration(hadoopConf, "metaInfoFolder")
  val queryNameFolder = getStringConfiguration(hadoopConf, "queryNameFolder")
  val queryPublishedFolder = getStringConfiguration(hadoopConf, "queryPublishedFolder")
  val queryUnpublishedFolder = getStringConfiguration(hadoopConf, "queryUnpublishedFolder")
  val namenode = getStringConfiguration(hadoopConf, "namenode")
  val parquetTablesFolder = getStringConfiguration(hadoopConf, "parquetTablesFolder")

  //app configuration
  val serverInterface = getStringConfiguration(appConf, "server.interface")
  val dalType = getStringConfiguration(appConf, "app.dal.type")
  val rddDestinationIp = getStringConfiguration(appConf, "rdd.destination.ip")
  val rddDestinationLocation = getStringConfiguration(appConf, "rdd.destination.location")
  val remoteDomainActor = getStringConfiguration(appConf, "remote.domain.actor")
  val applicationName = getStringConfiguration(appConf, "application.name")
  val webServicesPort = getStringConfiguration(appConf, "web.services.port")
  val webSocketsPort = getStringConfiguration(appConf, "web.sockets.port")
  val nrOfThreads = getStringConfiguration(appConf, "nr.of.threads")
  val timeout = getStringConfiguration(appConf, "timeout").getOrElse("10000").toInt
  val schemaFolder = getStringConfiguration(appConf, "schemaFolder")
  val numberOfResults = getStringConfiguration(appConf, "nr.of.results")
  val corsFilterAllowedHosts = getStringConfiguration(appConf, "cors-filter-allowed-hosts")
  val jarPath = getStringConfiguration(appConf, "jar-path")
  val hdfsNamenodePath = getStringConfiguration(appConf, "hdfs-namenode-path").getOrElse("")
  val tachyonNamenodePath = getStringConfiguration(appConf, "tachyon-namenode-path").getOrElse("")

  val LIMIT_EXCEPTION_MESSAGE = "The limit is null!"
  val SCRIPT_EXCEPTION_MESSAGE = "The script is empty or null!"
  val UUID_EXCEPTION_MESSAGE = "The uuid is empty or null!"
  val META_INFO_EXCEPTION_MESSAGE = "The metainfo is null!"
  val LIMITED_EXCEPTION_MESSAGE = "The limited flag is null!"
  val RESULTS_NUMBER_EXCEPTION_MESSAGE = "The results number is null!"
  val FILE_EXCEPTION_MESSAGE = "The file is null or empty!"
  val QUERY_NAME_MESSAGE = "The query name is null or empty!"
  val FILE_PATH_TYPE_EXCEPTION_MESSAGE = "The file path must be hdfs or tachyon"
  val DATABASE_EXCEPTION_MESSAGE = "The database is null or empty!"
  val TABLE_EXCEPTION_MESSAGE = "The table name is null or empty!"
  val PATH_IS_EMPTY = "Request parameter \'path\' must not be empty!"
  val TABLE_ALREADY_EXISTS_EXCEPTION_MESSAGE = "The table already exists!"
  val UNSUPPORTED_SOURCE_TYPE = "Unsupported value for parameter \'sourceType\' !"
  val UNSUPPORTED_STORAGE_TYPE = "Unsupported value for parameter \'storageType\' !"

  def getStringConfiguration(configuration: Config, configurationPath: String): Option[String] = {
    if (configuration.hasPath(configurationPath)) Option(configuration.getString(configurationPath).trim) else Option(null)
  }

}