package server

import java.net.InetAddress
import scala.collection.JavaConverters._
import com.typesafe.config.Config
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.log4j.Logger
import com.google.gson.Gson
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import customs.CORSDirectives
import spray.http.{ StatusCodes, HttpHeaders, HttpMethods, MediaTypes }
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.json.DefaultJsonProtocol._
import akka.actor._
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import spray.routing.directives.ParamDefMagnet.apply
import scala.util.{ Failure, Success, Try }
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.duration.Duration._
import apiactors.ListFilesActor
import apiactors.ListFilesMessage
import apiactors.ErrorMessage
import customs.Files

/**
 * Created by emaorhian
 */
object FileBrowserController extends App with SimpleRoutingApp with CORSDirectives {
  var hdfsConf: org.apache.hadoop.conf.Configuration = _
  implicit val timeout = Timeout(Configuration.timeout.toInt)
  implicit val localSystem: ActorSystem = ActorSystem("localSystem")
  val fileBrowserActor = localSystem.actorOf(Props[ListFilesActor], "listFilesActor");

  
  startServer(interface = Configuration.serverInterface.getOrElse(InetAddress.getLocalHost().getHostName()), port = Configuration.webServicesPort.getOrElse("8080").toInt) {
    pathPrefix("file-browser") {
      path("index") {
        get {

          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            complete {
              "File browser is up and running!"
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
        path("listFiles") {
          get {
            parameters('routePath.as[String]) { (routePath) =>
              corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
                validate(!routePath.trim.isEmpty, "Route path is empty!") {
                  respondWithMediaType(MediaTypes.`application/json`) { ctx =>
                    val future = ask(fileBrowserActor, new ListFilesMessage(routePath))
                    future.map {
                      case e: ErrorMessage => ctx.complete(StatusCodes.InternalServerError, e.message)
                      case result: Files   => ctx.complete(StatusCodes.OK, result)
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
  }
}

object Configuration {

  import com.typesafe.config.ConfigFactory

  val log4j = Logger.getLogger(FileBrowserController.getClass())

  private val conf = ConfigFactory.load
  conf.checkValid(ConfigFactory.defaultReference)

  val appConf = conf.getConfig("appConf")

  //app configuration
  val serverInterface = getStringConfiguration(appConf, "server.interface")
  val timeout = getStringConfiguration(appConf, "timeout").getOrElse("10000").toInt
  val corsFilterAllowedHosts = getStringConfiguration(appConf, "cors-filter-allowed-hosts")
  val webServicesPort = getStringConfiguration(appConf, "web.services.port")

  def getStringConfiguration(configuration: Config, configurationPath: String): Option[String] = {
    if (configuration.hasPath(configurationPath)) Option(configuration.getString(configurationPath).trim) else Option(null)
  }

}