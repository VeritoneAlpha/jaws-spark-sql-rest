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
  // Handles the parquet tables operations
  lazy val getParquetTablesActor: ActorRef = createActor(Props(new GetParquetTablesApiActor(hiveContext, dals)),
    GET_PARQUET_TABLES_ACTOR_NAME, localSupervisor)

  // Handles the registering of parquet tables
  lazy val registerParquetTableActor = createActor(Props(new RegisterParquetTableApiActor(hiveContext, dals)), REGISTER_PARQUET_TABLE_ACTOR_NAME, remoteSupervisor)

  /**
   * Initialize the parquet tables. Each time the server is started the parquet files, that are used as tables, are
   * registered as temporary table.
   */
  def initializeParquetTables() {
    Configuration.log4j.info("Initializing parquet tables on the current spark context")
    val parquetTables = dals.parquetTableDal.listParquetTables()

    parquetTables.foreach(pTable => {
      val newConf = new org.apache.hadoop.conf.Configuration(hdfsConf)
      newConf.set("fs.defaultFS", pTable.namenode)
      if (Utils.checkFileExistence(pTable.filePath, newConf)) {
        // Send message to register the parquet table
        val future = ask(registerParquetTableActor, RegisterTableMessage(pTable.name, pTable.filePath, pTable.namenode))

        // When registering is complete display the proper message or in case of failure delete the table.
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

  /**
   * Handles the calls to <b>/jaws/parquet/</b>
   */
  def parquetRoute: Route = pathPrefix("parquet") {
    parquetRunRoute ~ parquetTableRoute
  }

  /**
   * Handles the call to <b>/jaws/parquet/run</b>. This call is used to execute queries on parquet files. It returns
   * the query id that can be used to get the status of the query and its results.
   * The call is made with <b>POST</b> and it has the following parameters:
   * <ul>
   *  <li><b>tablePath</b>: the path to parquet folder to query</li>
   *  <li><b>pathType</b> [not required]: the type of parquet folder path. It can be <b>tachyon</b> or <b>hdfs</b>.
   *    The values for namenodes are filled from the configuration.</li>
   *  <li><b>table</b>: the table name for the parquet folder</li>
   *  <li><b>limited</b>: if set on <b>true</b> the result of the query is limited to a fixed number specified by
   *    parameter <b>numberOfResults</b>.</li>
   *  <li><b>numberOfResults</b> [not required]: it is taken in consideration only when the <b>limited</b> is <b>true</b></li>
   *  <li><b>overwrite</b> [not required, default <b>false</b>]: specifies whether the table is going to be overwritten
   *    if the table already exists.</li>
   * </ul>
   */
  private def parquetRunRoute = path("run") {
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
  }

  /**
   * Handles the management of the parquet tables operations: register, unregister and describe.
   * The following calls are handled:
   * <ul>
   *   <li>
   *     POST /jaws/parquet/tables
   *   </li>
   *   <li>
   *     GET /jaws/parquet/tables
   *   </li>
   *   <li>
   *    DELETE /jaws/parquet/tables/{name}
   *   </li>
   * </ul>
   */
  private def parquetTableRoute = pathPrefix("tables") {
    pathEnd {
      parquetTablePostRoute ~ parquetTableGetRoute
    } ~ parquetTableDeleteRoute ~
    options {
      corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*")), HttpHeaders.`Access-Control-Allow-Methods`(Seq(HttpMethods.OPTIONS, HttpMethods.POST, HttpMethods.DELETE, HttpMethods.GET))) {
        complete {
          "OK"
        }
      }
    }
  }

  /**
   * Handles call to <b>POST /jaws/parquet/tables</b>. It is used for registering a table from a parquet file.
   * The following parameters are used:
   * <ul>
   *  <li>
   *    <b>name</b>: the name of the table
   *  </li>
   *  <li>
   *    <b>path</b>: the path to the parquet folder
   *  </li>
   *  <li>
   *    <b>pathType</b> [not required]: the type of the path. It can be <b>tachyon</b> or <b>hdfs</b>.
   *    The values for namenodes are filled from the configuration.
   *  </li>
   *  <li>
   *    <b>overwrite</b> [not required, default <b>false</b>]: specifies whether the table is going to be
   *    overwritten if the table already exists.
   *  </li>
   * </ul>
   */
  private def parquetTablePostRoute = post {
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
  }

  /**
   * Handles call to <b>GET /jaws/parquet/tables<b>. It is used for getting information about the registered parquet
   * tables. If no table is specified, all the tables are returned.
   * The following parameters are used:
   * <ul>
   *  <li>
   *    <b>describe</b> [not required, default <b>false</b>]: flag for specifying whether the tables should be described
   *  </li>
   *  <li>
   *    <b>table</b> [not required]: the tables that should be described
   *  </li>
   * </ul>
   * It returns a list of tables information (this information is stored in [[com.xpatterns.jaws.data.DTO.Tables]])
   */
  private def parquetTableGetRoute = get {
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

  /**
   * Handles call to <b>DELETE /jaws/parquet/tables/{name}</b>. It is used for unregistering parquet tables.
   * The following parameter is used:
   * <ul>
   *  <li>
   *    <b>name</b>: the table that is unregistered.
   *  </li>
   * </ul>
   */
  private def parquetTableDeleteRoute = delete {
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
  }
}
