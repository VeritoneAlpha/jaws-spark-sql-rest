package server.api

import apiactors.ActorsPaths._
import apiactors.{RegisterParquetTableApiActor, GetParquetTablesApiActor}
import com.xpatterns.jaws.data.utils.Utils
import com.xpatterns.jaws.data.utils.Utils._
import server.Configuration
import server.MainActors._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{Props, ActorRef}
import akka.pattern.ask
import customs.CORSDirectives
import messages._
import com.xpatterns.jaws.data.DTO._
import spray.http.{ StatusCodes, HttpHeaders, HttpMethods, MediaTypes }
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive.pimpApply
import scala.concurrent.duration.Duration._
import scala.util.{Failure, Success, Try}
import scala.concurrent.{Await, Future}
import spray.routing.Route
import scala.collection.mutable.ArrayBuffer
import customs.CustomDirectives._


/**
 * Handles the api for parquet operations
 */
trait ParquetApi extends BaseApi with CORSDirectives {
  // Handles the parquet tables
  lazy val getParquetTablesActor: ActorRef = createActor(Props(new GetParquetTablesApiActor(hiveContext, dals)),
    GET_PARQUET_TABLES_ACTOR_NAME, localSupervisor)

  // Handles the registering of parquet tables
  lazy val registerParquetTableActor = createActor(Props(new RegisterParquetTableApiActor(hiveContext, dals)), REGISTER_PARQUET_TABLE_ACTOR_NAME, remoteSupervisor)

  def initializeParquetTables() {
    Configuration.log4j.info("Initializing parquet tables on the current spark context")
    val parquetTables = dals.parquetTableDal.listParquetTables()
    parquetTables.foreach(pTable => {
      val newConf = new org.apache.hadoop.conf.Configuration(hdfsConf)
      newConf.set("fs.defaultFS", pTable.namenode)
      if (Utils.checkFileExistence(pTable.filePath, newConf)) {
        val future = ask(registerParquetTableActor, RegisterTableMessage(pTable.name, pTable.filePath, pTable.namenode))
        Await.ready(future, Inf).value.get match {
          case Success(x) => x match {
            case e: ErrorMessage =>
              Configuration.log4j.warn(s"The table ${pTable.name} at path ${pTable.filePath} failed during registration with message : \n ${e.message}\n The table will be deleted!")
              dals.parquetTableDal.deleteParquetTable(pTable.name)

            case result: String => Configuration.log4j.info(result)
          }
          case Failure(ex) =>
            Configuration.log4j.warn(s"The table ${pTable.name} at path ${pTable.filePath} failed during registration with the following stack trace : \n ${getCompleteStackTrace(ex)}\n The table will be deleted!")
            dals.parquetTableDal.deleteParquetTable(pTable.name)
        }

      } else {
        Configuration.log4j.warn(s"The table ${pTable.name} doesn't exists at path ${pTable.filePath}. The table will be deleted")
        dals.parquetTableDal.deleteParquetTable(pTable.name)
      }
    })
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

                    params.foreach {
                      case ("describe", value)                    => describe = Try(value.toBoolean).getOrElse(false)
                      case ("table", value) if value.nonEmpty => tables += value
                      case (key, value)                           => Configuration.log4j.warn(s"Unknown parameter $key!")
                    }
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
}
