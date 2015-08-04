package server

import com.xpatterns.jaws.data.utils.GsonHelper._
import java.net.InetAddress
import com.xpatterns.jaws.data.utils.Utils._
import scala.collection.JavaConverters._
import com.typesafe.config.Config
import implementation.SchemaSettingsFactory.{ SourceType, StorageType }
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
import implementation.{ SchemaSettingsFactory }
import com.xpatterns.jaws.data.impl.CassandraDal
import com.xpatterns.jaws.data.impl.HdfsDal
import messages._
import com.xpatterns.jaws.data.DTO._
import spray.http.{ StatusCodes, HttpHeaders, HttpMethods, MediaTypes }
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import spray.routing.directives.ParamDefMagnet.apply
import com.xpatterns.jaws.data.contracts.DAL
import messages.GetResultsMessage
import scala.util.{ Failure, Success, Try }
import server.MainActors._
import org.apache.spark.scheduler.HiveUtils
import implementation.HiveContextWrapper
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.LoggingListener
import org.apache.spark.SparkConf
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.duration.Duration._
import spray.routing.Route
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import spray.json.RootJsonWriter
import spray.json.JsonPrinter
import spray.json.PrettyPrinter
import spray.httpx.marshalling.Marshaller
import spray.http.ContentTypes
import customs.CustomDirectives._
/**
 * Created by emaorhian
 */
object JawsController extends App with SimpleRoutingApp with CORSDirectives {
  var hdfsConf: org.apache.hadoop.conf.Configuration = _
  var hiveContext: HiveContextWrapper = _
  var dals: DAL = _

  initialize()
  implicit val timeout = Timeout(Configuration.timeout.toInt)

  // local actors
  val getQueriesActor = createActor(Props(new GetQueriesApiActor(dals)), GET_QUERIES_ACTOR_NAME, localSupervisor)
  val getTablesActor = createActor(Props(new GetTablesApiActor(hiveContext, dals)), GET_TABLES_ACTOR_NAME, localSupervisor)
  val getLogsActor = createActor(Props(new GetLogsApiActor(dals)), GET_LOGS_ACTOR_NAME, localSupervisor)
  val getResultsActor = createActor(Props(new GetResultsApiActor(hdfsConf, hiveContext, dals)), GET_RESULTS_ACTOR_NAME, localSupervisor)
  val getDatabasesActor = createActor(Props(new GetDatabasesApiActor(hiveContext, dals)), GET_DATABASES_ACTOR_NAME, localSupervisor)
  val getDatasourceSchemaActor = createActor(Props(new GetDatasourceSchemaActor(hiveContext)), GET_DATASOURCE_SCHEMA_ACTOR_NAME, localSupervisor)
  val deleteQueryActor = createActor(Props(new DeleteQueryApiActor(dals)), DELETE_QUERY_ACTOR_NAME, localSupervisor)
  val getParquetTablesActor = createActor(Props(new GetParquetTablesApiActor(hiveContext, dals)), GET_PARQUET_TABLES_ACTOR_NAME, localSupervisor)
  val queryNameApiActor = createActor(Props(new QueryNameApiActor(dals)), QUERY_NAME_ACTOR_NAME, localSupervisor)

  //remote actors
  val runScriptActor = createActor(Props(new RunScriptApiActor(hdfsConf, hiveContext, dals)), RUN_SCRIPT_ACTOR_NAME, remoteSupervisor)
  val balancerActor = createActor(Props(classOf[BalancerActor]), BALANCER_ACTOR_NAME, remoteSupervisor)
  val registerParquetTableActor = createActor(Props(new RegisterParquetTableApiActor(hiveContext, dals)), REGISTER_PARQUET_TABLE_ACTOR_NAME, remoteSupervisor)

  //initialize parquet tables
  initializeParquetTables

  implicit val spraySystem: ActorSystem = ActorSystem("spraySystem")

  def uiRoute: Route = pathPrefix("ui") {
    pathSingleSlash {
      get {
        getFromResource("webapp/index.html")
      } ~ options {
        corsFilter(List("*"), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
          complete {
            "OK"
          }
        }
      }
    } ~
      pathEnd {
        redirect("ui/", StatusCodes.PermanentRedirect)
      } ~
      get {
        getFromResourceDirectory("webapp")
      } ~
      options {
        corsFilter(List("*"), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET))) {
          complete {
            "OK"
          }
        }
      }
  }

  def indexRoute: Route = path("index") {
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
  }

  private def getNamenodeFromPathType(pathType:String):String = {
    if ("hdfs".equals(pathType)) {
      Configuration.hdfsNamenodePath
    } else if ("tachyon".equals(pathType)) {
      Configuration.tachyonNamenodePath
    } else {
      ""
    }
  }

  def parquetRoute: Route = pathPrefix("parquet") {
    path("run") {
      post {
        parameters('tablePath.as[String], 'pathType.as[String] ? "hdfs", 'table.as[String], 'numberOfResults.as[Int] ? 100,
          'limited.as[Boolean], 'destination.as[String] ? Configuration.rddDestinationLocation.getOrElse("hdfs"),
          'overwrite.as[Boolean] ? false) {
            (tablePath, pathType, table, numberOfResults, limited, destination, overwrite) =>
              corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              validateCondition(tablePath != null && !tablePath.trim.isEmpty, Configuration.FILE_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                validateCondition("hdfs".equals(pathType) || "tachyon".equals(pathType), Configuration.FILE_PATH_TYPE_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                  validateCondition(table != null && !table.trim.isEmpty, Configuration.TABLE_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                    entity(as[String]) { query: String =>
                      validateCondition(query != null && !query.trim.isEmpty, Configuration.SCRIPT_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                        validateCondition(overwrite || !dals.parquetTableDal.tableExists(table), Configuration.TABLE_ALREADY_EXISTS_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                          respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                            Configuration.log4j.info(s"The tablePath is $tablePath on namenode $pathType and the table name is $table")
                            val future = ask(runScriptActor, RunParquetMessage(query, tablePath, getNamenodeFromPathType(pathType), table, limited, numberOfResults, destination))
                            future.map {
                              case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                              case result: String  => ctx.complete(StatusCodes.OK, result)
                            }
                          }
                        }
                      }
                    }
                  }
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
      pathPrefix("tables") {
        pathEnd {
          post {
            parameters('name.as[String], 'path.as[String], 'pathType.as[String] ? "hdfs", 'overwrite.as[Boolean] ? false) {
              (name, path, pathType, overwrite) =>
                corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
                  validateCondition(path != null && !path.trim.isEmpty, Configuration.FILE_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                    validateCondition("hdfs".equals(pathType) || "tachyon".equals(pathType), Configuration.FILE_PATH_TYPE_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                      validateCondition(name != null && !name.trim.isEmpty, Configuration.TABLE_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                        validateCondition(overwrite || !dals.parquetTableDal.tableExists(name), Configuration.TABLE_ALREADY_EXISTS_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                          respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                            Configuration.log4j.info(s"Registering table $name having the path $path on node $pathType")

                            val future = ask(balancerActor, RegisterTableMessage(name, path, getNamenodeFromPathType(pathType)))
                              .map(innerFuture => innerFuture.asInstanceOf[Future[Any]])
                              .flatMap(identity)
                            future.map {
                              case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                              case result: String => ctx.complete(StatusCodes.OK, result)
                            }
                          }
                        }
                      }
                    }
                  }
                }
            }
          } ~
            get {
              parameterSeq { params =>
                corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {

                  respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                    var tables = ArrayBuffer[String]()
                    var describe = false

                    params.foreach(touple => touple match {
                      case ("describe", value)                    => describe = Try(value.toBoolean).getOrElse(false)
                      case ("table", value) if (!value.isEmpty()) => tables += value
                      case (key, value)                           => Configuration.log4j.warn(s"Unknown parameter $key!")
                    })
                    Configuration.log4j.info(s"Retrieving table information for parquet tables= $tables")
                    val future = ask(getParquetTablesActor, new GetParquetTablesMessage(tables.toArray, describe))

                    future.map {
                      case e: ErrorMessage       => ctx.complete(StatusCodes.InternalServerError, e.message)
                      case result: Array[Tables] => ctx.complete(StatusCodes.OK, result)
                    }
                  }
                }
              }
            }
        } ~
          delete {
            path(Segment) { name =>
              corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
                {
                  validateCondition(name != null && !name.trim.isEmpty, Configuration.TABLE_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                    respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                      Configuration.log4j.info(s"Unregistering table $name ")
                      val future = ask(balancerActor, UnregisterTableMessage(name))
                        .map(innerFuture => innerFuture.asInstanceOf[Future[Any]])
                        .flatMap(identity)
                      future.map {
                        case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                        case result: String  => ctx.complete(StatusCodes.OK, result)
                      }
                    }
                  }
                }
              }
            }
          } ~
          options {
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.POST, HttpMethods.DELETE, HttpMethods.GET))) {
              complete {
                "OK"
              }
            }
          }
      }
  }

  def runManagementRoute: Route = path("run") {
    post {
      parameters('numberOfResults.as[Int] ? 100, 'limited.as[Boolean], 'destination.as[String] ? Configuration.rddDestinationLocation.getOrElse("hdfs")) { (numberOfResults, limited, destination) =>
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {

          entity(as[String]) { query: String =>
            validateCondition(query != null && !query.trim.isEmpty(), Configuration.SCRIPT_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
              respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                Configuration.log4j.info(s"The query is limited=$limited and the destination is $destination")
                val future = ask(runScriptActor, RunScriptMessage(query, limited, numberOfResults, destination.toLowerCase()))
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                  case result: String  => ctx.complete(StatusCodes.OK, result)
                }
              }
            }
          }
        }
      } ~
        parameters('name.as[String]) { (queryName) =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            validateCondition(queryName != null && !queryName.trim.isEmpty, Configuration.QUERY_NAME_MESSAGE, StatusCodes.BadRequest) {
              respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                Configuration.log4j.info(s"Running the query with name $queryName")
                val future = ask(runScriptActor, RunQueryMessage(queryName.trim))
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                  case result: String => ctx.complete(StatusCodes.OK, result)
                }
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
    path("logs") {
      get {
        parameters('queryID, 'startTimestamp.as[Long].?, 'limit.as[Int]) { (queryID, startTimestamp, limit) =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            validateCondition(queryID != null && !queryID.trim.isEmpty(), Configuration.UUID_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                var timestamp: java.lang.Long = 0L
                if (startTimestamp.isDefined) {
                  timestamp = startTimestamp.get
                }
                val future = ask(getLogsActor, GetLogsMessage(queryID, timestamp, limit))
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                  case result: Logs    => ctx.complete(StatusCodes.OK, result)
                }
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
    path("results") {
      get {
        parameters('queryID, 'offset.as[Int], 'limit.as[Int], 'format ? "default") { (queryID, offset, limit, format) =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            validateCondition(queryID != null && !queryID.trim.isEmpty(), Configuration.UUID_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>

                implicit def customResultMarshaller[T] =
                  Marshaller.delegate[T, String](ContentTypes.`application/json`) { value ⇒
                    customGson.toJson(value)
                  }

                val future = ask(getResultsActor, GetResultsMessage(queryID, offset, limit, format.toLowerCase()))
                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                  case result: Any     => ctx.complete(StatusCodes.OK, result)
                }
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
    pathPrefix("queries") {
      pathEnd {
        get {
          parameterSeq { params =>

            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                var limit: Int = 100
                var startQueryID: String = null
                val queries = ListBuffer[String]()
                var queryName: String = null

                params.foreach {
                  case ("limit", value)                         => limit = Try(value.toInt).getOrElse(100)
                  case ("startQueryID", value)                  => startQueryID = Option(value).orNull
                  case ("queryID", value) if value.nonEmpty     => queries += value
                  case ("name", value) if value.trim.nonEmpty   => queryName = value.trim()
                  case (key, value)                             => Configuration.log4j.warn(s"Unknown parameter $key!")
                }

                val future = if (queryName != null && queryName.nonEmpty) {
                    ask(getQueriesActor, GetQueriesByName(queryName))
                  } else if (queries.isEmpty) {
                    ask(getQueriesActor, GetPaginatedQueriesMessage(startQueryID, limit))
                  } else {
                    ask(getQueriesActor, GetQueriesMessage(queries))
                  }

                future.map {
                  case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                  case result: Queries => ctx.complete(StatusCodes.OK, result)
                }
              }
            }
          }
        }
      } ~
        put {
          (path(Segment) & entity(as[QueryMetaInfo]) & parameter("overwrite".as[Boolean] ? false)) {
            (queryID, metaInfo, overwrite) =>
              corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
                validateCondition(queryID != null && !queryID.trim.isEmpty, Configuration.UUID_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                  validateCondition(metaInfo != null, Configuration.META_INFO_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                    respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                      val future = ask(queryNameApiActor, new UpdateQueryNameMessage(queryID, metaInfo.name, metaInfo.description, overwrite))
                      future.map {
                        case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                        case message: String => ctx.complete(StatusCodes.OK, message)
                      }
                    }
                  }
                }
              }
          }
        } ~
          delete {
            path(Segment) { queryID =>
              corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
                validateCondition(queryID != null && !queryID.trim.isEmpty, Configuration.UUID_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                  respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                    val future = ask(deleteQueryActor, new DeleteQueryMessage(queryID))
                    future.map {
                      case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                      case message: String => ctx.complete(StatusCodes.OK, message)
                    }
                  }
                }
              }
            }
          } ~
          options {
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.GET, HttpMethods.DELETE))) {
              complete {
                "OK"
              }
            }
          }
    } ~
    path("cancel") {
      post {
        parameters('queryID.as[String]) { queryID =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            complete {
              balancerActor ! CancelMessage(queryID)

              Configuration.log4j.info("Cancel message was sent")
              "Cancel message was sent"
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
    }

  def tablesRoute: Route = pathPrefix("tables") {
    pathEnd {
      get {
        parameterSeq { params =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {

            respondWithMediaType(MediaTypes.`application/json`) { ctx =>

              val (database, describe, tables) = getTablesParameters(params)

              Configuration.log4j.info(s"Retrieving table information for database=$database, tables= $tables, with describe flag set on: $describe")
              val future = ask(getTablesActor, new GetTablesMessage(database, describe, tables))

              future.map {
                case e: ErrorMessage       => ctx.complete(StatusCodes.InternalServerError, e.message)
                case result: Array[Tables] => ctx.complete(StatusCodes.OK, result)
              }
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
      } ~
      path("extended") {
        get {
          parameterSeq { params =>
            corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
              val (database, describe, tables) = getTablesParameters(params)
              validateCondition(database != null && !database.trim.isEmpty, Configuration.DATABASE_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                respondWithMediaType(MediaTypes.`application/json`) {
                  ctx =>

                    Configuration.log4j.info(s"Retrieving extended table information for database=$database, tables= $tables")
                    val future = ask(getTablesActor, new GetExtendedTablesMessage(database, tables))

                    future.map {
                      case e: ErrorMessage       => ctx.complete(StatusCodes.InternalServerError, e.message)
                      case result: Array[Tables] => ctx.complete(StatusCodes.OK, result)
                      case _                     => ctx.complete(StatusCodes.Accepted, "Other")
                    }
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
      path("formatted") {
        get {
          parameterSeq {
            params =>
              corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
                val (database, describe, tables) = getTablesParameters(params)
                validateCondition(database != null && !database.trim.isEmpty, Configuration.DATABASE_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
                  respondWithMediaType(MediaTypes.`application/json`) {
                    ctx =>
                      Configuration.log4j.info(s"Retrieving formatted table information for database=$database, tables= $tables")
                      val future = ask(getTablesActor, new GetFormattedTablesMessage(database, tables.toArray))

                      future.map {
                        case e: ErrorMessage       => ctx.complete(StatusCodes.InternalServerError, e.message)
                        case result: Array[Tables] => ctx.complete(StatusCodes.OK, result)
                      }
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

  def metadataRoute: Route = pathPrefix("hive") {
    tablesRoute ~ path("databases") {

      get {
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {

          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            val future = ask(getDatabasesActor, GetDatabasesMessage())
            future.map {
              case e: ErrorMessage   => ctx.complete(StatusCodes.InternalServerError, e.message)
              case result: Databases => ctx.complete(StatusCodes.OK, result)
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
  } ~
    path("schema") {
      get {
        parameters('path.as[String], 'sourceType.as[String], 'storageType.?) { (path, sourceType, storageType) =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            validateCondition(!path.trim.isEmpty, Configuration.PATH_IS_EMPTY, StatusCodes.BadRequest) {
              var validSourceType: SourceType = null
              var validStorageType: StorageType = null
              val validateParams = Try {
                validSourceType = SchemaSettingsFactory.getSourceType(sourceType)
                validStorageType = SchemaSettingsFactory.getStorageType(storageType.getOrElse("hdfs"))
              }
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                validateParams match {
                  case Failure(e) =>
                    Configuration.log4j.error(e.getMessage)
                    ctx.complete(StatusCodes.InternalServerError, e.getMessage)
                  case Success(_) =>
                    val schemaRequest: GetDatasourceSchemaMessage = GetDatasourceSchemaMessage(path, validSourceType, validStorageType)
                    val future = ask(getDatasourceSchemaActor, schemaRequest)
                    future.map {
                      case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                      case result: String =>
                        Configuration.log4j.info("Getting the data source schema was successful!")
                        ctx.complete(StatusCodes.OK, result)
                    }
                }
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

  startServer(interface = Configuration.serverInterface.getOrElse(InetAddress.getLocalHost().getHostName()),
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
    Utils.createFolderIfDoesntExist(hdfsConf, Configuration.schemaFolder.getOrElse("jawsSchemaFolder"), false)

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
        property <- config.entrySet().asScala if (property.getKey.startsWith("spark") && property.getValue() != null)
      ) {
        val key = property.getKey().replaceAll("-", ".");
        println(key + " | " + property.getValue.unwrapped())
        sparkConf.set(key, property.getValue.unwrapped().toString)
      }
      sparkConf
    }

    val hContext: HiveContextWrapper = {
      var sparkConf = configToSparkConf(Configuration.sparkConf, Configuration.applicationName.getOrElse("Jaws"), jars)
      var sContext = new SparkContext(sparkConf)

      var hContext = new HiveContextWrapper(sContext)
      hContext.sparkContext.addSparkListener(new LoggingListener(dal))

      HiveUtils.setSharkProperties(hContext, this.getClass().getClassLoader().getResourceAsStream("sharkSettings.txt"))
      //make sure that lazy variable hiveConf gets initialized
      hContext.runMetadataSql("use default")
      hContext
    }
    hContext
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
    configuration.set(Utils.PARQUET_TABLES_FOLDER, Configuration.parquetTablesFolder.getOrElse("parquetTablesFolder"))

    return configuration
  }

  def initializeParquetTables() {
    Configuration.log4j.info("Initializing parquet tables on the current spark context")
    val parquetTables = dals.parquetTableDal.listParquetTables
    parquetTables.foreach(pTable => {
      val newConf = new org.apache.hadoop.conf.Configuration(hdfsConf)
      newConf.set("fs.defaultFS", pTable.namenode)
      if (Utils.checkFileExistence(pTable.filePath, newConf)) {
        val future = ask(registerParquetTableActor, RegisterTableMessage(pTable.name, pTable.filePath, pTable.namenode))
        Await.ready(future, Inf).value.get match {
          case Success(x) => x match {
            case e: ErrorMessage => {
              Configuration.log4j.warn(s"The table ${pTable.name} at path ${pTable.filePath} failed during registration with message : \n ${e.message}\n The table will be deleted!")
              dals.parquetTableDal.deleteParquetTable(pTable.name)
            }
            case result: String => Configuration.log4j.info(result)
          }
          case Failure(ex) => {
            Configuration.log4j.warn(s"The table ${pTable.name} at path ${pTable.filePath} failed during registration with the following stack trace : \n ${getCompleteStackTrace(ex)}\n The table will be deleted!")
            dals.parquetTableDal.deleteParquetTable(pTable.name)
          }
        }

      } else {
        Configuration.log4j.warn(s"The table ${pTable.name} doesn't exists at path ${pTable.filePath}. The table will be deleted")
        dals.parquetTableDal.deleteParquetTable(pTable.name)
      }
    })
  }

  private def getTablesParameters(params: Seq[(String, String)]): Tuple3[String, Boolean, Array[String]] = {
    var database = ""
    var describe = false
    var tables = ArrayBuffer[String]()
    params.foreach(touple => touple match {
      case ("database", value)                    => database = Option(value).getOrElse("")
      case ("describe", value)                    => describe = Try(value.toBoolean).getOrElse(false)
      case ("table", value) if (!value.isEmpty()) => tables += value
      case (key, value)                           => Configuration.log4j.warn(s"Unknown parameter $key!")
    })

    (database, describe, tables.toArray)
  }
}

object Configuration {

  import com.typesafe.config.ConfigFactory

  val log4j = Logger.getLogger(JawsController.getClass())

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
  val namenode = getStringConfiguration(hadoopConf, "namenode")
  val parquetTablesFolder = getStringConfiguration(hadoopConf, "parquetTablesFolder")

  //app configuration
  val serverInterface = getStringConfiguration(appConf, "server.interface")
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