package server

import java.net.InetAddress
import com.typesafe.config.Config
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.log4j.Logger
import com.google.gson.Gson
import com.xpatterns.jaws.data.utils.Utils
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import apiactors._
import apiactors.ActorsPaths._
import customs.CORSDirectives
import implementation.CassandraDal
import implementation.HdfsDal
import messages._
import com.xpatterns.jaws.data.DTO.Queries
import spray.http.{StatusCodes, HttpHeaders, HttpMethods, MediaTypes}
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import spray.routing.directives.ParamDefMagnet.apply
import traits.DAL
import messages.GetResultsMessage
import implementation.CustomHiveContextCreator
import com.xpatterns.jaws.data.DTO.Logs
import com.xpatterns.jaws.data.DTO.Result
import com.xpatterns.jaws.data.DTO.Query
import scala.util.Try

/**
 * Created by emaorhian
 */
object JawsController extends App with SimpleRoutingApp with MainActors with Systems with CORSDirectives {
  var hdfsConf: org.apache.hadoop.conf.Configuration = _
  var customSharkContext: CustomHiveContextCreator = _
  var dals: DAL = _

  def initialize() = {
    Configuration.log4j.info("Initializing...")

    hdfsConf = getHadoopConf
    Utils.createFolderIfDoesntExist(hdfsConf, Configuration.schemaFolder.getOrElse("jawsSchemaFolder"), false)

    Configuration.loggingType.getOrElse("cassandra") match {
      case "cassandra" => dals = new CassandraDal()
      case _ => dals = new HdfsDal(hdfsConf)
    }

    customSharkContext = new CustomHiveContextCreator(dals)
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

    configuration.set(Utils.LOGGING_FOLDER, Configuration.loggingFolder.getOrElse("jawsLogs"))
    configuration.set(Utils.STATUS_FOLDER, Configuration.stateFolder.getOrElse("jawsStates"))
    configuration.set(Utils.DETAILS_FOLDER, Configuration.detailsFolder.getOrElse("jawsDetails"))
    configuration.set(Utils.METAINFO_FOLDER, Configuration.metaInfoFolder.getOrElse("jawsMetainfoFolder"))
    configuration.set(Utils.RESULTS_FOLDER, Configuration.resultsFolder.getOrElse("jawsResultsFolder"))

    return configuration
  }

  initialize()
  implicit val timeout = Timeout(Configuration.timeout.toInt)

  val getQueriesActor = createActor(Props(new GetQueriesApiActor(dals)), GET_QUERIES_ACTOR_NAME)
  val getTablesActor = createActor(Props(new GetTablesApiActor(customSharkContext.hiveContext, dals)), GET_TABLES_ACTOR_NAME)
  val runScriptActor = createActor(Props(new RunScriptApiActor(hdfsConf, customSharkContext.hiveContext, dals)), RUN_SCRIPT_ACTOR_NAME, domainSystem)
  val getLogsActor = createActor(Props(new GetLogsApiActor(dals)), GET_LOGS_ACTOR_NAME)
  val getResultsActor = createActor(Props(new GetResultsApiActor(hdfsConf, customSharkContext.hiveContext, dals)), GET_RESULTS_ACTOR_NAME)
  val getQueryInfoActor = createActor(Props(new GetQueryInfoApiActor(dals)), GET_QUERY_INFO_ACTOR_NAME)
  val getDatabasesActor = createActor(Props(new GetDatabasesApiActor(customSharkContext.hiveContext, dals)), GET_DATABASES_ACTOR_NAME)
  val cancelActor = createActor(Props(classOf[CancelActor], runScriptActor), CANCEL_ACTOR_NAME, cancelSystem)

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
          parameters('numberOfResults.as[Int] ? 100, 'limited.as[Boolean], 'destination.as[String] ? Configuration.rddDestinationLocation.getOrElse("hdfs")) { (numberOfResults, limited, destination) =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              entity(as[String]) { query: String =>
                complete {
                  Configuration.log4j.info(s"The queryis limited=$limited and the destination is $destination")
                  val future = ask(runScriptActor, RunScriptMessage(query, limited, numberOfResults, destination.toLowerCase())).mapTo[String]
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

              var timestamp: java.lang.Long = 0
              if (startTimestamp.isDefined) {
                timestamp = startTimestamp.get
              }
              val future = ask(getLogsActor, GetLogsMessage(queryID, timestamp, limit))
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.BadRequest, e.message)
                  case result: Logs => ctx.complete(StatusCodes.OK, result)
                }
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
              val future = ask(getResultsActor, GetResultsMessage(queryID, offset, limit))
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.BadRequest, e.message)
                  case result: Result => ctx.complete(StatusCodes.OK, result)
                }
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
              val future = ask(getQueriesActor, GetQueriesMessage(startQueryID.getOrElse(null), limit))
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.BadRequest, e.message)
                  case result: Queries => ctx.complete(StatusCodes.OK, result)
                }
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
              val future = ask(getQueryInfoActor, GetQueryInfoMessage(queryID))
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.BadRequest, e.message)
                  case result: Query => ctx.complete(StatusCodes.OK, result)
                }
              }
            }
          }
        }
      } ~
      path(pathPrefix / "databases") {
        get {
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {

            val future = ask(getDatabasesActor, GetDatabasesMessage())
            respondWithMediaType(MediaTypes.`application/json`) { ctx =>
              future.map {
                case e: ErrorMessage => ctx.complete(StatusCodes.BadRequest, e.message)
                case result: Result => ctx.complete(StatusCodes.OK, result)
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
          parameterMultiMap { params =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              var database = ""
              var describe = true
              var tables: List[String] = List()

              params.foreach(touple => touple._1 match {
                case "database" => {
                  if (touple._2 != null && !touple._2.isEmpty)
                    database = touple._2(0)
                }
                case "describe" => {
                  if (touple._2 != null && !touple._2.isEmpty)
                    describe = Try(touple._2(0).toBoolean).getOrElse(true)
                }
                case "tables" => {
                  tables = touple._2
                }
                case _ => Configuration.log4j.warn(s"Unknown parameter ${touple._1}!")
              })
              Configuration.log4j.info(s"Retrieving table information for database=$database, tables= $tables, with describe flag set on: $describe")
              val future = ask(getTablesActor, new GetTablesMessage(database, describe, tables))

              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.BadRequest, e.message)
                  case result: Map[String, Map[String, Result]] => ctx.complete(StatusCodes.OK, result)
                }
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
              val future = ask(getTablesActor, new GetExtendedTablesMessage(database, table.getOrElse("")))
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.BadRequest, e.message)
                  case result: Map[String, Map[String, Result]] => ctx.complete(StatusCodes.OK, result)
                }
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
              val future = ask(getTablesActor, new GetFormattedTablesMessage(database, table.getOrElse("")))
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.BadRequest, e.message)
                  case result: Map[String, Map[String, Result]] => ctx.complete(StatusCodes.OK, result)
                }
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

  val domain = conf.getConfig("domain")
  val cancel = conf.getConfig("cancel")
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
  val namenode = getStringConfiguration(hadoopConf, "namenode")

  //app configuration
  val loggingType = getStringConfiguration(appConf, "app.logging.type")
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