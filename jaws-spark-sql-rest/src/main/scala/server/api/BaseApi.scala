package server.api

import akka.actor.{Props, ActorRef}
import akka.util.Timeout
import apiactors.{BalancerActor, RunScriptApiActor}
import com.xpatterns.jaws.data.contracts.DAL
import implementation.HiveContextWrapper
import server.Configuration
import spray.routing.HttpService
import server.MainActors._
import apiactors.ActorsPaths._

/**
 * The base trait api
 */
trait BaseApi extends HttpService {
  implicit val timeout = Timeout(Configuration.timeout.toInt)

  var hdfsConf: org.apache.hadoop.conf.Configuration = _
  var hiveContext: HiveContextWrapper = _

  var dals: DAL = _

  // The actor that is handling the scripts that are run on Hive or Spark SQL
  lazy val runScriptActor = createActor(Props(new RunScriptApiActor(hdfsConf, hiveContext, dals)), RUN_SCRIPT_ACTOR_NAME, remoteSupervisor)

  // The actor that is handling the parquet tables
  lazy val balancerActor = createActor(Props(classOf[BalancerActor]), BALANCER_ACTOR_NAME, remoteSupervisor)

  /**
   * @param pathType the path type of the requested name node
   * @return the proper namenode path
   */
  protected def getNamenodeFromPathType(pathType:String):String = {
    if ("hdfs".equals(pathType)) {
      Configuration.hdfsNamenodePath
    } else if ("tachyon".equals(pathType)) {
      Configuration.tachyonNamenodePath
    } else {
      ""
    }
  }
}
