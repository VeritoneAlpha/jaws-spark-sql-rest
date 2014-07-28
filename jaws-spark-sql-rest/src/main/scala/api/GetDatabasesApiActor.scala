package api

import akka.actor.Actor
import akka.actor.actorRef2Scala
import messages.GetQueriesMessage
import com.google.common.base.Preconditions
import actors.LogsActor
import akka.actor.ActorLogging
import traits.DAL
import messages.GetDatabasesMessage
import java.util.UUID
import actors.Configuration
import com.xpatterns.jaws.data.DTO.Result
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.scheduler.HiveUtils

/**
 * Created by emaorhian
 */
class GetDatabasesApiActor(hiveContext: HiveContext, dals: DAL) extends Actor{

  override def receive = {

    case message: GetDatabasesMessage => {
      Configuration.log4j.info("[GetDatabasesApiActor]: showing databases")
      val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
      val databases = HiveUtils.runCmd("show databases", hiveContext, uuid, dals.loggingDal)

      sender ! Result.trimResults(databases)
    }
  }
}
