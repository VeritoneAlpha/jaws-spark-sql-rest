package apiactors

import akka.actor.Actor
import akka.actor.actorRef2Scala
import apiactors.ActorOperations._
import messages.GetQueriesMessage
import com.google.common.base.Preconditions
import server.LogsActor
import akka.actor.ActorLogging
import traits.DAL
import messages.GetDatabasesMessage
import java.util.UUID
import server.Configuration
import com.xpatterns.jaws.data.DTO.Result
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.scheduler.HiveUtils
import implementation.HiveContextWrapper

import scala.util.Try

/**
 * Created by emaorhian
 */
class GetDatabasesApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor{

  override def receive = {

    case message: GetDatabasesMessage => {
      Configuration.log4j.info("[GetDatabasesApiActor]: showing databases")
      var result:Result = null
      val tryGetDatabases = Try {
        val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
        val databases = HiveUtils.runMetadataCmd(hiveContext, "show databases", dals.loggingDal, uuid)

        result = Result.trimResults(databases)
      }
      returnResult(tryGetDatabases, result, "GET databases failed with the following message: ", sender)
    }
  }
}
