package actors

import java.net.InetAddress
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import akka.util.Timeout
import spray.http.HttpHeaders
import spray.http.HttpMethods
import spray.httpx.SprayJsonSupport._
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import spray.routing.directives.ParamDefMagnet.apply
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.Logger


/**
 * Created by emaorhian
 */
object JawsController extends App with SimpleRoutingApp {
  var hdfsConf: org.apache.hadoop.conf.Configuration = _

  def initialize() = {
    Configuration.log4j.info("Initializing...")
    System.getProperties().setProperty("spark.executor.memory", Configuration.sparkExecutorMemory.getOrElse("4g"))
    System.getProperties().setProperty("spark.scheduler.mode", Configuration.sparkSchedulerMode.getOrElse("FAIR"))
    System.getProperties().setProperty("spark.mesos.coarse", Configuration.sparkMesosCoarse.getOrElse("false"))
    System.getProperties().setProperty("spark.cores.max", Configuration.sparkCoresMax.getOrElse("2"))

    System.getProperties().setProperty("spark.shuffle.spill", Configuration.sparkShuffleSpill.getOrElse("false"))
    System.getProperties().setProperty("spark.default.parallelism", Configuration.sparkDefaultParallelism.getOrElse("384"))
    System.getProperties().setProperty("spark.storage.memoryFraction", Configuration.sparkStorageMemoryFraction.getOrElse("0.3"))
    System.getProperties().setProperty("spark.shuffle.memoryFraction", Configuration.sparkShuffleMemoryFraction.getOrElse("0.6"))
    System.getProperties().setProperty("spark.shuffle.compress", Configuration.sparkShuffleCompress.getOrElse("true"))
    System.getProperties().setProperty("spark.shuffle.spill.compress", Configuration.sparkShuffleSpillCompress.getOrElse("true"))
    System.getProperties().setProperty("spark.reducer.maxMbInFlight", Configuration.sparkReducerMaxMbInFlight.getOrElse("48"))
    System.getProperties().setProperty("spark.akka.frameSize", Configuration.sparkAkkaFrameSize.getOrElse("false"))
    System.getProperties().setProperty("spark.akka.threads", Configuration.sparkAkkaThreads.getOrElse("4"))
    System.getProperties().setProperty("spark.akka.timeout", Configuration.sparkAkkaTimeout.getOrElse("100"))
    System.getProperties().setProperty("spark.task.maxFailures", Configuration.sparkTaskMaxFailures.getOrElse("4"))
    System.getProperties().setProperty("spark.shuffle.consolidateFiles", Configuration.sparkShuffleConsolidateFiles.getOrElse("true"))
    System.getProperties().setProperty("spark.deploy.spreadOut", Configuration.sparkDeploySpreadOut.getOrElse("true"))

  }

  def getHadoopConf(): org.apache.hadoop.conf.Configuration = {
    val configuration = new org.apache.hadoop.conf.Configuration()
    

    // set hadoop name node and job tracker
    Configuration.namenode match {
      case None => {
        val message = "You need to set the namenode! "
        Configuration.log4j.error(message)
        throw new RuntimeException(message)
      }
      case _ => configuration.set("fs.defaultFS", Configuration.namenode.get)

    }

    configuration.set("dfs.replication", Configuration.replicationFactor.getOrElse("1"))

    return configuration
  }

  initialize()
  implicit val timeout = Timeout(Configuration.timeout.toInt)
  val jars = Array(Configuration.jarPath.get)

  var sContext = new SparkContext(Configuration.sparkMaster.get, Configuration.applicationName.getOrElse("Jaws"), Configuration.sparkPath.get, jars.toSeq, Map.empty)

  var hiveContext = new HiveContext(sContext)
  hiveContext.hql("show databases").collect
  
  hiveContext.hql("use test")
  hiveContext.hql("show tables")
  val resultRdd = hiveContext.hql("select * from varsta")
 
  val result = resultRdd.collect
  
   println (resultRdd.schemaString)
  result.foreach(println)

}
object Configuration {
  import com.typesafe.config.ConfigFactory

  val log4j = Logger.getLogger(JawsController.getClass())

  private val conf = ConfigFactory.load
  conf.checkValid(ConfigFactory.defaultReference)

  val domain = conf.getConfig("domain").withFallback(conf)
  val cancel = conf.getConfig("cancel").withFallback(conf)
  val sparkConf = conf.getConfig("sparkConfiguration").withFallback(conf)
  val appConf = conf.getConfig("appConf").withFallback(conf)
  val hadoopConf = conf.getConfig("hadoopConf").withFallback(conf)
  val cassandraConf = conf.getConfig("cassandraConf").withFallback(conf)

  // cassandra configuration
  val cassandraHost = Option(cassandraConf.getString("cassandra.host"))
  val cassandraKeyspace = Option(cassandraConf.getString("cassandra.keyspace"))
  val cassandraClusterName = Option(cassandraConf.getString("cassandra.cluster.name"))

  //hadoop conf 
  val replicationFactor = Option(hadoopConf.getString("replicationFactor"))
  val forcedMode = Option(hadoopConf.getString("forcedMode"))
  val loggingFolder = Option(hadoopConf.getString("loggingFolder"))
  val stateFolder = Option(hadoopConf.getString("stateFolder"))
  val detailsFolder = Option(hadoopConf.getString("detailsFolder"))
  val resultsFolder = Option(hadoopConf.getString("resultsFolder"))
  val metaInfoFolder = Option(hadoopConf.getString("metaInfoFolder"))
  val namenode = Option(hadoopConf.getString("namenode"))

  //app configuration
  val loggingType = Option(appConf.getString("app.logging.type"))
  val jawsNamenode = Option(appConf.getString("jaws.namenode"))
  val remoteDomainActor = Option(appConf.getString("remote.domain.actor").trim())
  val applicationName = Option(appConf.getString("application.name"))
  val webServicesPort = Option(appConf.getString("web.services.port"))
  val webSocketsPort = Option(appConf.getString("web.sockets.port"))
  val nrOfThreads = Option(appConf.getString("nr.of.threads"))
  val timeout = Option(appConf.getString("timeout")).getOrElse("10000").toInt
  val schemaFolder = Option(appConf.getString("schemaFolder"))
  val numberOfResults = Option(appConf.getString("nr.of.results"))
  val corsFilterAllowedHosts = Option(appConf.getString("cors-filter-allowed-hosts"))
  val jarPath = Option(appConf.getString("jar-path"))

  //spark configuration
  val sparkMaster = Option(sparkConf.getString("spark-master"))
  val sparkPath = Option(sparkConf.getString("spark-path"))
  val sparkExecutorMemory = Option(sparkConf.getString("spark-executor-memory"))
  val sparkSchedulerMode = Option(sparkConf.getString("spark-scheduler-mode"))
  val sparkMesosCoarse = Option(sparkConf.getString("spark-mesos-coarse"))
  val sparkCoresMax = Option(sparkConf.getString("spark-cores-max"))
  val sparkShuffleSpill = Option(sparkConf.getString("spark-shuffle-spill"))
  val sparkDefaultParallelism = Option(sparkConf.getString("spark-default-parallelism"))
  val sparkStorageMemoryFraction = Option(sparkConf.getString("spark-storage-memoryFraction"))
  val sparkShuffleMemoryFraction = Option(sparkConf.getString("spark-shuffle-memoryFraction"))
  val sparkShuffleCompress = Option(sparkConf.getString("spark-shuffle-compress"))
  val sparkShuffleSpillCompress = Option(sparkConf.getString("spark-shuffle-spill-compress"))
  val sparkReducerMaxMbInFlight = Option(sparkConf.getString("spark-reducer-maxMbInFlight"))
  val sparkAkkaFrameSize = Option(sparkConf.getString("spark-akka-frameSize"))
  val sparkAkkaThreads = Option(sparkConf.getString("spark-akka-threads"))
  val sparkAkkaTimeout = Option(sparkConf.getString("spark-akka-timeout"))
  val sparkTaskMaxFailures = Option(sparkConf.getString("spark-task-maxFailures"))
  val sparkShuffleConsolidateFiles = Option(sparkConf.getString("spark-shuffle-consolidateFiles"))
  val sparkDeploySpreadOut = Option(sparkConf.getString("spark-deploy-spreadOut"))

  val LIMIT_EXCEPTION_MESSAGE: Any = "The limit is null!"
  val HQL_SCRIPT_EXCEPTION_MESSAGE: Any = "The hqlScript is empty or null!"
  val UUID_EXCEPTION_MESSAGE: Any = "The uuid is empty or null!"
  val LIMITED_EXCEPTION_MESSAGE: Any = "The limited flag is null!"
  val RESULSTS_NUMBER_EXCEPTION_MESSAGE: Any = "The results number is null!"
}