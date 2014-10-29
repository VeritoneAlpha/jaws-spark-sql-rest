package actors

import java.net.InetAddress
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.log4j.Logger
import com.google.gson.Gson
import com.typesafe.config.ConfigFactory
import com.xpatterns.jaws.data.utils.Utils
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import akka.util.Timeout
import api.GetDatabasesApiActor
import api.GetQueryInfoApiActor
import api.GetQueriesApiActor
import api.GetLogsApiActor
import api.GetResultsApiActor
import api.GetTablesApiActor
import api.RunScriptApiActor
import customs.CORSDirectives
import implementation.CassandraDal
import implementation.CustomSharkContextCreator
import implementation.HdfsDal
import messages._
import model.Queries
import model.Logs
import model.Result
import spray.http.HttpHeaders
import spray.http.HttpMethods
import spray.httpx.SprayJsonSupport._
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import spray.routing.directives.ParamDefMagnet.apply
import traits.CustomSharkContext
import traits.DAL
import messages.GetResultsMessage
import model.QueryInfo
import com.typesafe.config.Config

/**
 * Created by emaorhian
 */
object JawsController extends App with SimpleRoutingApp with MainActors with Systems with CORSDirectives {
  var hdfsConf: org.apache.hadoop.conf.Configuration = _
  var customSharkContext: CustomSharkContext = _
  var dals: DAL = _

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
    System.getProperties().setProperty("spark.kryo.referenceTracking", Configuration.sparkKryoReferenceTracking.getOrElse("true"))

    // *******Set the kryo properties only if they exist: NO DEFAULTS FOR IT
    Configuration.sparkSerializer match {
      case None => Configuration.log4j.info("spark.serializer configuration not set!")
      case _ => System.getProperties().setProperty("spark.serializer", Configuration.sparkSerializer.get)
    }
    Configuration.sparkKryosSerializerBufferMb match {
      case None => Configuration.log4j.info("spark.kryoserializer.buffer.mb configuration not set!")
      case _ => System.getProperties().setProperty("spark.kryoserializer.buffer.mb", Configuration.sparkKryosSerializerBufferMb.get)
    }
    Configuration.sparkKryoSerializerBufferMaxMb match {
      case None => Configuration.log4j.info("spark.kryoserializer.buffer.max.mb configuration not set!")
      case _ => System.getProperties().setProperty("spark.kryoserializer.buffer.max.mb", Configuration.sparkKryoSerializerBufferMaxMb.get)
    }
    // *******
    
    hdfsConf = getHadoopConf
    Utils.createFolderIfDoesntExist(hdfsConf, Configuration.schemaFolder.getOrElse("jawsSchemaFolder"), false)

    Configuration.loggingType.getOrElse("cassandra") match {
      case "cassandra" => dals = new CassandraDal()
      case _ => dals = new HdfsDal(hdfsConf)
    }

    customSharkContext = new CustomSharkContextCreator(dals)
  }

  def getHadoopConf(): org.apache.hadoop.conf.Configuration = {
    val configuration = new org.apache.hadoop.conf.Configuration()
    configuration.setBoolean(Utils.FORCED_MODE, Configuration.forcedMode.getOrElse("false").toBoolean)

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

    configuration.set(Utils.LOGGING_FOLDER, Configuration.loggingFolder.getOrElse("jawsLogs"));
    configuration.set(Utils.STATUS_FOLDER, Configuration.stateFolder.getOrElse("jawsStates"));
    configuration.set(Utils.DETAILS_FOLDER, Configuration.detailsFolder.getOrElse("jawsDetails"));
    configuration.set(Utils.METAINFO_FOLDER, Configuration.metaInfoFolder.getOrElse("jawsMetainfoFolder"));
    configuration.set(Utils.RESULTS_FOLDER, Configuration.resultsFolder.getOrElse("jawsResultsFolder"));

    return configuration
  }

  initialize()
  implicit val timeout = Timeout(Configuration.timeout.toInt)

  val getQueriesActor = createActor(Props(new GetQueriesApiActor(dals)), "GetQueries", system)
  val runScriptActor = createActor(Props(new RunScriptApiActor(hdfsConf, customSharkContext, dals)), "RunScript", domainSystem)
  val getLogsActor = createActor(Props(new GetLogsApiActor(dals)), "GetLogs", system)
  val getResultsActor = createActor(Props(new GetResultsApiActor(hdfsConf, customSharkContext, dals)), "GetResults", system)
  val getQueryInfoActor = createActor(Props(new GetQueryInfoApiActor(dals)), "GetQueryInfo", system)
  val getDatabasesActor = createActor(Props(new GetDatabasesApiActor(customSharkContext, dals)), "GetDatabases", system)
  val cancelActor = createActor(Props(classOf[CancelActor], runScriptActor), "Cancel", cancelSystem)
  val getTablesActor = createActor(Props(new GetTablesApiActor(customSharkContext, dals)), "GetTables", system)

  val gson = new Gson()
  val pathPrefix = "jaws"

  startServer(interface = InetAddress.getLocalHost().getHostName(), port = Configuration.webServicesPort.getOrElse("8080").toInt) {
    path(pathPrefix / "index") {
      get {

        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
          complete {
            "Jaws is up and running!"
          }
        }

      } ~
        options {
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
            complete {
              "OK"
            }
          }
        }
    } ~
      path(pathPrefix / "run") {
        post {
          parameters('numberOfResults.as[Int] ? 100, 'limited.as[Boolean]) { (numberOfResults, limited) =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              entity(as[String]) { string: String =>
                complete {
                  val future = ask(runScriptActor, RunScriptMessage(string, limited, numberOfResults)).mapTo[String]
                  future
                }
              }
            }
          }
        } ~
          options {
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.POST))) {
              complete {
                "OK"
              }
            }
          }
      } ~
      path(pathPrefix / "logs") {
        get {
          parameters('queryID, 'startTimestamp.as[Long].?, 'limit.as[Int]) { (queryID, startTimestamp, limit) =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              complete {
                var timestamp: java.lang.Long = 0
                if (startTimestamp.isDefined) {
                  timestamp = startTimestamp.get
                }
                val future = ask(getLogsActor, GetLogsMessage(queryID, timestamp, limit)).mapTo[Logs]
                future
              }
            }
          }
        } ~
          options {
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
              complete {
                "OK"
              }
            }
          }
      } ~
      path(pathPrefix / "results") {
        get {
          parameters('queryID, 'offset.as[Int], 'limit.as[Int]) { (queryID, offset, limit) =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              complete {
                val future = ask(getResultsActor, GetResultsMessage(queryID, offset, limit)).mapTo[Result]
                future

              }
            }
          }
        } ~
          options {
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
              complete {
                "OK"
              }
            }
          }
      } ~
      path(pathPrefix / "queries") {
        get {
          parameters('startQueryID.?, 'limit.as[Int]) { (startQueryID, limit) =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              complete {
                val future = ask(getQueriesActor, GetQueriesMessage(startQueryID.getOrElse(null), limit)).mapTo[Queries]
                future
              }
            }
          }
        } ~
          options {
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
              complete {
                "OK"
              }
            }
          }
      } ~
      path(pathPrefix / "queryInfo") {
        get {
          parameters('queryID) { queryID =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              complete {
                val future = ask(getQueryInfoActor, GetQueryInfoMessage(queryID)).mapTo[QueryInfo]
                future
              }
            }
          }
        }
      } ~
      path(pathPrefix / "databases") {
        get {
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            complete {
              val future = ask(getDatabasesActor, GetDatabasesMessage()).mapTo[Result]
              future
            }
          }
        } ~
          options {
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
              complete {
                "OK"
              }
            }
          }
      } ~
      path(pathPrefix / "cancel") {
        post {
          parameters('queryID.as[String]) { queryID =>
            complete {
              cancelActor ! CancelMessage(queryID)

              Configuration.log4j.info("Cancel message was sent")
              "Cancel message was sent"
            }
          }
        } ~
          options {
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.POST))) {
              complete {
                "OK"
              }
            }
          }
      } ~
      path(pathPrefix / "tables") {
        get {
          parameters('database.?) { database =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              complete {

                val future = ask(getTablesActor, new GetTablesMessage(database.getOrElse(null))).mapTo[scala.collection.immutable.Map[String, scala.collection.immutable.Map[String, Result]]]
                future
              }
            }
          }
        } ~
          options {
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
              complete {
                "OK"
              }
            }
          }
      } ~
      path(pathPrefix / "tables" / "extended") {
        get {
          parameters('database.as[String], 'table.?) { (database, table) =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              complete {

                val future = ask(getTablesActor, new GetExtendedTablesMessage(database, table.getOrElse(""))).mapTo[scala.collection.immutable.Map[String, scala.collection.immutable.Map[String, Result]]]
                future
              }
            }
          }
        } ~
          options {
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
              complete {
                "OK"
              }
            }
          }
      } ~
      path(pathPrefix / "tables" / "formatted") {
        get {
          parameters('database.as[String], 'table.?) { (database, table) =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              complete {

                val future = ask(getTablesActor, new GetFormattedTablesMessage(database, table.getOrElse(""))).mapTo[scala.collection.immutable.Map[String, scala.collection.immutable.Map[String, Result]]]
                future
              }
            }
          }
        } ~
          options {
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
              complete {
                "OK"
              }
            }
          }
      }

  }

  private val reactiveServer = new ReactiveServer(Configuration.webSocketsPort.getOrElse("8081").toInt, logsActor)
  reactiveServer.start()

}

trait Systems {
  implicit lazy val system: ActorSystem = ActorSystem("system")
  def cancelSystem: ActorSystem = ActorSystem("cancelSystem", Configuration.cancel)
  def domainSystem: ActorSystem = ActorSystem("domainSystem", Configuration.domain)

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
  val namenode = getStringConfiguration(hadoopConf, "namenode")

  //app configuration
  val loggingType = getStringConfiguration(appConf, "app.logging.type")
  val jawsNamenode = getStringConfiguration(appConf, "jaws.namenode")
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

  //spark configuration
  val sparkMaster = getStringConfiguration(sparkConf, "spark-master")
  val sparkPath = getStringConfiguration(sparkConf, "spark-path")
  val sparkExecutorMemory = getStringConfiguration(sparkConf, "spark-executor-memory")
  val sparkSchedulerMode = getStringConfiguration(sparkConf, "spark-scheduler-mode")
  val sparkMesosCoarse = getStringConfiguration(sparkConf, "spark-mesos-coarse")
  val sparkCoresMax = getStringConfiguration(sparkConf, "spark-cores-max")
  val sparkShuffleSpill = getStringConfiguration(sparkConf, "spark-shuffle-spill")
  val sparkDefaultParallelism = getStringConfiguration(sparkConf, "spark-default-parallelism")
  val sparkStorageMemoryFraction = getStringConfiguration(sparkConf, "spark-storage-memoryFraction")
  val sparkShuffleMemoryFraction = getStringConfiguration(sparkConf, "spark-shuffle-memoryFraction")
  val sparkShuffleCompress = getStringConfiguration(sparkConf, "spark-shuffle-compress")
  val sparkShuffleSpillCompress = getStringConfiguration(sparkConf, "spark-shuffle-spill-compress")
  val sparkReducerMaxMbInFlight = getStringConfiguration(sparkConf, "spark-reducer-maxMbInFlight")
  val sparkAkkaFrameSize = getStringConfiguration(sparkConf, "spark-akka-frameSize")
  val sparkAkkaThreads = getStringConfiguration(sparkConf, "spark-akka-threads")
  val sparkAkkaTimeout = getStringConfiguration(sparkConf, "spark-akka-timeout")
  val sparkTaskMaxFailures = getStringConfiguration(sparkConf, "spark-task-maxFailures")
  val sparkShuffleConsolidateFiles = getStringConfiguration(sparkConf, "spark-shuffle-consolidateFiles")
  val sparkDeploySpreadOut = getStringConfiguration(sparkConf, "spark-deploy-spreadOut")
  val sparkSerializer = getStringConfiguration(sparkConf, "spark-serializer")
  val sparkKryosSerializerBufferMb = getStringConfiguration(sparkConf, "spark-kryoserializer-buffer-mb")
  val sparkKryoSerializerBufferMaxMb = getStringConfiguration(sparkConf, "spark-kryoserializer-buffer-max-mb")
  val sparkKryoReferenceTracking = getStringConfiguration(sparkConf, "spark-kryo-referenceTracking")

  val LIMIT_EXCEPTION_MESSAGE: Any = "The limit is null!"
  val HQL_SCRIPT_EXCEPTION_MESSAGE: Any = "The hqlScript is empty or null!"
  val UUID_EXCEPTION_MESSAGE: Any = "The uuid is empty or null!"
  val LIMITED_EXCEPTION_MESSAGE: Any = "The limited flag is null!"
  val RESULSTS_NUMBER_EXCEPTION_MESSAGE: Any = "The results number is null!"

  def getStringConfiguration(configuration: Config, configurationPath: String): Option[String] = {
    return if (configuration.hasPath(configurationPath)) Option(configuration.getString(configurationPath).trim) else Option(null)
  }
}