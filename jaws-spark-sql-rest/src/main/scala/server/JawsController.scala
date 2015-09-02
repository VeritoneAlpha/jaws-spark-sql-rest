package server

import java.net.InetAddress
import server.api._
import scala.collection.JavaConverters._
import com.typesafe.config.Config
import com.xpatterns.jaws.data.utils.Utils
import akka.actor.ActorSystem
import customs.CORSDirectives
import com.xpatterns.jaws.data.impl.CassandraDal
import com.xpatterns.jaws.data.impl.HdfsDal
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import com.xpatterns.jaws.data.contracts.DAL
import org.apache.spark.scheduler.HiveUtils
import implementation.HiveContextWrapper
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.LoggingListener
import org.apache.spark.SparkConf

/**
 * Created by emaorhian
 */
object JawsController extends App with BaseApi with UIApi with IndexApi with ParquetApi with MetadataApi
  with QueryManagementApi with SimpleRoutingApp with CORSDirectives {
  initialize()

  // initialize parquet tables
  initializeParquetTables()

  implicit val spraySystem: ActorSystem = ActorSystem("spraySystem")

  startServer(interface = Configuration.serverInterface.getOrElse(InetAddress.getLocalHost.getHostName),
    port = Configuration.webServicesPort.getOrElse("8080").toInt) {
      pathPrefix("jaws") {
        uiRoute ~ indexRoute ~ parquetRoute ~ metadataRoute ~ runManagementRoute
      }
    }

  private val reactiveServer = new ReactiveServer(Configuration.webSocketsPort.getOrElse("8081").toInt, MainActors.logsActor)
  reactiveServer.start()

  def initialize() = {
    Configuration.log4j.info("Initializing...")

    hdfsConf = getHadoopConf
    Utils.createFolderIfDoesntExist(hdfsConf, Configuration.schemaFolder.getOrElse("jawsSchemaFolder"), forcedMode = false)

    Configuration.loggingType.getOrElse("cassandra") match {
      case "cassandra" => dals = new CassandraDal(Configuration.cassandraHost.get, Configuration.cassandraClusterName.get, Configuration.cassandraKeyspace.get)
      case _           => dals = new HdfsDal(hdfsConf)
    }

    hiveContext = createHiveContext(dals)
  }

  def createHiveContext(dal: DAL): HiveContextWrapper = {
    val jars = Array(Configuration.jarPath.get)

    def configToSparkConf(config: Config, contextName: String, jars: Array[String]): SparkConf = {
      val sparkConf = new SparkConf().setAppName(contextName).setJars(jars)
      for (
        property <- config.entrySet().asScala if property.getKey.startsWith("spark") && property.getValue != null
      ) {
        val key = property.getKey.replaceAll("-", ".")
        println(key + " | " + property.getValue.unwrapped())
        sparkConf.set(key, property.getValue.unwrapped().toString)
      }
      sparkConf
    }

    val hContext: HiveContextWrapper = {
      val sparkConf = configToSparkConf(Configuration.sparkConf, Configuration.applicationName.getOrElse("Jaws"), jars)
      val sContext = new SparkContext(sparkConf)

      val hContext = new HiveContextWrapper(sContext)
      hContext.sparkContext.addSparkListener(new LoggingListener(dal))

      HiveUtils.setSharkProperties(hContext, this.getClass.getClassLoader.getResourceAsStream("sharkSettings.txt"))
      //make sure that lazy variable hiveConf gets initialized
      hContext.runMetadataSql("use default")
      hContext
    }
    hContext
  }

  def getHadoopConf: org.apache.hadoop.conf.Configuration = {
    val configuration = new org.apache.hadoop.conf.Configuration()
    configuration.setBoolean(Utils.FORCED_MODE, Configuration.forcedMode.getOrElse("false").toBoolean)

    // set hadoop name node and job tracker
    Configuration.namenode match {
      case None =>
        val message = "You need to set the namenode! "
        Configuration.log4j.error(message)
        throw new RuntimeException(message)

      case _ => configuration.set("fs.defaultFS", Configuration.namenode.get)

    }

    configuration.set("dfs.replication", Configuration.replicationFactor.getOrElse("1"))

    configuration.set(Utils.LOGGING_FOLDER, Configuration.loggingFolder.getOrElse("jawsLogs"))
    configuration.set(Utils.STATUS_FOLDER, Configuration.stateFolder.getOrElse("jawsStates"))
    configuration.set(Utils.DETAILS_FOLDER, Configuration.detailsFolder.getOrElse("jawsDetails"))
    configuration.set(Utils.METAINFO_FOLDER, Configuration.metaInfoFolder.getOrElse("jawsMetainfoFolder"))
    configuration.set(Utils.QUERY_NAME_FOLDER, Configuration.queryNameFolder.getOrElse("jawsQueryNameFolder"))
    configuration.set(Utils.QUERY_PUBLISHED_FOLDER, Configuration.queryPublishedFolder.getOrElse("jawsQueryPublishedFolder"))
    configuration.set(Utils.QUERY_UNPUBLISHED_FOLDER, Configuration.queryUnpublishedFolder.getOrElse("jawsQueryUnpublishedFolder"))
    configuration.set(Utils.RESULTS_FOLDER, Configuration.resultsFolder.getOrElse("jawsResultsFolder"))
    configuration.set(Utils.PARQUET_TABLES_FOLDER, Configuration.parquetTablesFolder.getOrElse("parquetTablesFolder"))

    configuration
  }
}
