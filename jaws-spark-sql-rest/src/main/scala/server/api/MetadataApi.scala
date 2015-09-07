package server.api

import apiactors.ActorsPaths._
import apiactors.{GetDatasourceSchemaActor, GetDatabasesApiActor, GetTablesApiActor}
import customs.CustomDirectives._
import implementation.SchemaSettingsFactory
import implementation.SchemaSettingsFactory.{StorageType, SourceType}
import server.Configuration
import server.MainActors._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Props
import akka.pattern.ask
import customs.CORSDirectives
import messages._
import com.xpatterns.jaws.data.DTO._
import spray.http.{ StatusCodes, HttpHeaders, HttpMethods, MediaTypes }
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive.pimpApply
import scala.util.{Success, Failure, Try}
import scala.collection.mutable.ArrayBuffer
/**
 * Handles the tables api requests
 */
trait MetadataApi extends BaseApi with CORSDirectives {
  // Actor used for getting metadata about the tables
  lazy val getTablesActor = createActor(Props(new GetTablesApiActor(hiveContext, dals)), GET_TABLES_ACTOR_NAME, localSupervisor)

  // Actor used for getting information about the databases
  lazy val getDatabasesActor = createActor(Props(new GetDatabasesApiActor(hiveContext, dals)), GET_DATABASES_ACTOR_NAME, localSupervisor)

  // Actor used for getting information about the schema
  lazy val getDatasourceSchemaActor = createActor(Props(new GetDatasourceSchemaActor(hiveContext)), GET_DATASOURCE_SCHEMA_ACTOR_NAME, localSupervisor)

  /**
   * Manages the calls used for getting the metadata about tables, databases and its schema. It handles the following
   * calls:
   * <ul>
   *   <li>/jaws/hive/tables</li>
   *   <li>/jaws/hive/databases</li>
   *   <li>/jaws/schema</li>
   * </ul>
   */
  def hiveSchemaRoute = pathPrefix("hive") {
    hiveDatabasesRoute ~ hiveTablesRoute
  } ~ schemaRoute

  /**
   * Handles the call to <b>GET /jaws/hive/databases</b>.
   * It returns a list with the existing databases (the information is stored in [[com.xpatterns.jaws.data.DTO.Databases]])
   */
  private def hiveDatabasesRoute = path("databases") {
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

  /**
   * Used to get information about the tables. The following calls are handled:
   * <ul>
   *   <li>
   *     GET /jaws/hive/tables
   *   </li>
   *   <li>
   *     GET /jaws/hive/tables/extended
   *   </li>
   *   <li>
   *     GET /jaws/hive/tables/formatted
   *   </li>
   * </ul>
   */
  private def hiveTablesRoute = pathPrefix("tables") {
    hiveTablesDefaultRoute ~ hiveTablesExtendedRoute ~ hiveTablesFormattedRoute
  }

  /**
   * Handles the call to <b>GET /jaws/hive/tables</b>. It is used for getting basic info about tables.
   * The following parameters are used:
   * <ul>
   *  <li>
   *    <b>database</b> [not required]: the database for which the information about tables is retrieved
   *  </li>
   *  <li>
   *    <b>describe</b> [not required, default <b>true</b>]: flag that specifies whether the tables should be described
   *  </li>
   *  <li>
   *    <b>table</b> [not required]: the list of table that should be described
   *  </li>
   * </ul>
   * It returns an array with the information about tables. The information is stored in [[com.xpatterns.jaws.data.DTO.Tables]]
   */
  private def hiveTablesDefaultRoute = pathEnd {
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
  }

  /**
   * Handles the call to <b>GET /jaws/hive/tables/extended</b>. It is used for getting extended information about tables.
   * The following parameters are used:
   * <ul>
   *  <li>
   *    <b>database</b> [not required]: the database for which the information about tables is retrieved
   *  </li>
   *  <li>
   *    <b>table</b> [not required]: the list of table that should be described
   *  </li>
   * </ul>
   * It returns an array with the information about tables. The information is stored in [[com.xpatterns.jaws.data.DTO.Tables]].
   * The extended information is stored in the field <b>extraInfo</b>
   */
  private def hiveTablesExtendedRoute = path("extended") {
    get {
      parameterSeq { params =>
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
          val (database, _, tables) = getTablesParameters(params)
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
  }

  /**
   * Handles the call to <b>GET /jaws/hive/tables/formatted</b>. This call is used for getting formatted info about tables.
   * The following parameters are used:
   * <ul>
   *  <li>
   *    <b>database</b> [not required]: the database for which the information about tables is retrieved
   *  </li>
   *  <li>
   *    <b>table</b> [not required]: the list of table that should be described
   *  </li>
   * </ul>
   * It returns an array with the information about tables. The information is stored in [[com.xpatterns.jaws.data.DTO.Tables]].
   * The extended information is stored in the field <b>extraInfo</b>
   */
  private def hiveTablesFormattedRoute = path("formatted") {
    get {
      parameterSeq {
        params =>
          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            val (database, _, tables) = getTablesParameters(params)
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

  /**
   * Handles the call to <b>GET /jaws/schema</b>. The following parameters are used:
   * <ul>
   *   <li>
   *     <b>path</b>: the location of data.
   *   </li>
   *   <li>
   *     <b>sourceType</b>: the source of data; it can be <b>HIVE</b> or <b>PARQUET</b>
   *   </li>
   *   <li>
   *     <b>storageType</b> [not required, default <b>HDFS</b>]: the persistence layer where the <b>PARQUET<b> data is stored.
   *   </li>
   * </ul>
   * It returns a JSON containing the schema in AVRO format.
   */
  private def schemaRoute = path("schema") {
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
                  val schemaRequest: GetDatasourceSchemaMessage = GetDatasourceSchemaMessage(path, validSourceType, validStorageType, hdfsConf)
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

  /**
   * Extracts the tables parameters from the http request
   * @param params the parameters sent through http request
   * @return a tuple with the information about database, describe flag and tables
   */
  private def getTablesParameters(params: Seq[(String, String)]) = {
    var database = ""
    var describe = false
    var tables = ArrayBuffer[String]()
    params.foreach {
      case ("database", value)                    => database = Option(value).getOrElse("")
      case ("describe", value)                    => describe = Try(value.toBoolean).getOrElse(false)
      case ("table", value) if value.nonEmpty     => tables += value
      case (key, value)                           => Configuration.log4j.warn(s"Unknown parameter $key!")
    }

    (database, describe, tables.toArray)
  }
}
