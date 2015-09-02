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
import spray.routing.Route
import scala.collection.mutable.ArrayBuffer
/**
 * Handles the tables api requests
 */
trait MetadataApi extends BaseApi with CORSDirectives {
  lazy val getTablesActor = createActor(Props(new GetTablesApiActor(hiveContext, dals)), GET_TABLES_ACTOR_NAME, localSupervisor)
  lazy val getDatabasesActor = createActor(Props(new GetDatabasesApiActor(hiveContext, dals)), GET_DATABASES_ACTOR_NAME, localSupervisor)
  lazy val getDatasourceSchemaActor = createActor(Props(new GetDatasourceSchemaActor(hiveContext)), GET_DATASOURCE_SCHEMA_ACTOR_NAME, localSupervisor)


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
