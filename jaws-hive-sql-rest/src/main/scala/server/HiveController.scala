package server

import java.net.InetAddress
import scala.collection.JavaConverters._
import com.typesafe.config.Config
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.log4j.Logger
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
import apiactors.ErrorMessage
import apiactors.HiveRunnerActor
import apiactors.RunQueryMessage
import com.xpatterns.jaws.data.impl.CassandraDal
import com.xpatterns.jaws.data.impl.HdfsDal
import com.xpatterns.jaws.data.utils.Utils

/**
 * Created by emaorhian
 */
object HiveController extends App with SimpleRoutingApp with CORSDirectives {

  implicit val timeout = Timeout(Configuration.timeout.toInt)
  implicit val localSystem: ActorSystem = ActorSystem("localSystem")

  val hdfsConf: org.apache.hadoop.conf.Configuration = getHadoopConf()
  val dal = Configuration.loggingType.getOrElse("cassandra") match {
    case "cassandra" => new CassandraDal(Configuration.cassandraHost.get, Configuration.cassandraClusterName.get, Configuration.cassandraKeyspace.get)
    case _ => new HdfsDal(hdfsConf)
  }

  val hiveRunnerActor = localSystem.actorOf(Props(new HiveRunnerActor(dal)), "hiveRunnerActor");
  startServer(interface = Configuration.serverInterface.getOrElse(InetAddress.getLocalHost().getHostName()), port = Configuration.webServicesPort.getOrElse("8080").toInt) {

    pathPrefix("jaws" / "hive") {
      path("index") {
        get {

          corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
            complete {
              "Jaws hive sql rest is up and running!"
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
        path("run") {
          post {
            parameters('limit.as[Int] ? 100) { (limit) =>
              corsFilter(List(Configuration.corsFilterAllowedHosts.getOrElse("*"))) {
                validate(limit > 0, "Limit must be higher than 0") {
                  entity(as[String]) { query: String =>
                    validate(query != null && !query.trim.isEmpty(), "Script is empty!") {
                      respondWithMediaType(MediaTypes.`text/plain`) { ctx =>
                        Configuration.log4j.info(s"The query has the limit=$limit")
                        val future = ask(hiveRunnerActor, RunQueryMessage(query, limit))
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
    configuration.set(Utils.QUERY_NAME_FOLDER, Configuration.queryNameFolder.getOrElse("jawsQueryNameFolder"))
    configuration.set(Utils.QUERY_PUBLISHED_FOLDER, Configuration.queryPublishedFolder.getOrElse("jawsQueryPublishedFolder"))
    configuration.set(Utils.QUERY_UNPUBLISHED_FOLDER, Configuration.queryUnpublishedFolder.getOrElse("jawsQueryUnpublishedFolder"))
    configuration.set(Utils.RESULTS_FOLDER, Configuration.resultsFolder.getOrElse("jawsResultsFolder"))
    configuration.set(Utils.PARQUET_TABLES_FOLDER, Configuration.parquetTablesFolder.getOrElse("parquetTablesFolder"))

    return configuration
  }

}

object Configuration {

  import com.typesafe.config.ConfigFactory

  val log4j = Logger.getLogger(HiveController.getClass())

  private val conf = ConfigFactory.load
  conf.checkValid(ConfigFactory.defaultReference)

  val appConf = conf.getConfig("appConf")
  val cassandraConf = conf.getConfig("cassandraConf")
  val hadoopConf = conf.getConfig("hadoopConf")

  //app configuration
  val serverInterface = getStringConfiguration(appConf, "server.interface")
  val timeout = getStringConfiguration(appConf, "timeout").getOrElse("10000").toInt
  val corsFilterAllowedHosts = getStringConfiguration(appConf, "cors-filter-allowed-hosts")
  val webServicesPort = getStringConfiguration(appConf, "web.services.port")
  val loggingType = getStringConfiguration(appConf, "app.logging.type")
  val nrOfThreads = getStringConfiguration(appConf, "nr.of.threads")

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
  val queryNameFolder = getStringConfiguration(hadoopConf, "queryNameFolder")
  val queryPublishedFolder = getStringConfiguration(hadoopConf, "queryPublishedFolder")
  val queryUnpublishedFolder = getStringConfiguration(hadoopConf, "queryUnpublishedFolder")
  val namenode = getStringConfiguration(hadoopConf, "namenode")
  val parquetTablesFolder = getStringConfiguration(hadoopConf, "parquetTablesFolder")

  def getStringConfiguration(configuration: Config, configurationPath: String): Option[String] = {
    if (configuration.hasPath(configurationPath)) Option(configuration.getString(configurationPath).trim) else Option(null)
  }

}