package server.api

import apiactors.ActorsPaths._
import apiactors._
import com.xpatterns.jaws.data.utils.GsonHelper._
import server.Configuration
import server.MainActors._
import spray.httpx.marshalling.Marshaller
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Props
import akka.pattern.ask
import customs.CORSDirectives
import messages._
import com.xpatterns.jaws.data.DTO._
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive.pimpApply
import scala.util.Try
import spray.routing.Route
import scala.collection.mutable.ListBuffer
import customs.CustomDirectives._

/**
 * Handles the query management api
 */
trait QueryManagementApi extends BaseApi with CORSDirectives {
  lazy val getResultsActor = createActor(Props(new GetResultsApiActor(hdfsConf, hiveContext, dals)), GET_RESULTS_ACTOR_NAME, localSupervisor)
  lazy val getLogsActor = createActor(Props(new GetLogsApiActor(dals)), GET_LOGS_ACTOR_NAME, localSupervisor)
  lazy val deleteQueryActor = createActor(Props(new DeleteQueryApiActor(dals)), DELETE_QUERY_ACTOR_NAME, localSupervisor)
  lazy val getQueriesActor = createActor(Props(new GetQueriesApiActor(dals)), GET_QUERIES_ACTOR_NAME, localSupervisor)
  lazy val queryPropertiesApiActor = createActor(Props(new QueryPropertiesApiActor(dals)), QUERY_NAME_ACTOR_NAME, localSupervisor)



  def runManagementRoute: Route = path("run") {
    post {
      parameters('numberOfResults.as[Int] ? 100, 'limited.as[Boolean], 'destination.as[String] ? Configuration.rddDestinationLocation.getOrElse("hdfs")) { (numberOfResults, limited, destination) =>
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {

          entity(as[String]) { query: String =>
            validateCondition(query != null && !query.trim.isEmpty, Configuration.SCRIPT_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
              respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                Configuration.log4j.info(s"The query is limited=$limited and the destination is $destination")
                val future = ask(runScriptActor, RunScriptMessage(query, limited, numberOfResults, destination.toLowerCase))
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
            validateCondition(queryID != null && !queryID.trim.isEmpty, Configuration.UUID_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
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
            validateCondition(queryID != null && !queryID.trim.isEmpty, Configuration.UUID_EXCEPTION_MESSAGE, StatusCodes.BadRequest) {
              respondWithMediaType(MediaTypes.`application/json`) { ctx =>

                implicit def customResultMarshaller[T] =
                  Marshaller.delegate[T, String](ContentTypes.`application/json`) { value ⇒
                    customGson.toJson(value)
                  }

                val future = ask(getResultsActor, GetResultsMessage(queryID, offset, limit, format.toLowerCase))
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
    path("queries" / "published") {
      get {
        corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            val future = ask(getQueriesActor, GetPublishedQueries())

            future.map {
              case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
              case result:Array[String] => ctx.complete(StatusCodes.OK, result)
            }
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
                      val future = ask(queryPropertiesApiActor, new UpdateQueryPropertiesMessage(queryID, metaInfo.name,
                        metaInfo.description, metaInfo.published, overwrite))
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


}
