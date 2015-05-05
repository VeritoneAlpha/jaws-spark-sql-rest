package apiactors

import akka.actor.Actor
import akka.actor.actorRef2Scala
import apiactors.ActorOperations._
import com.google.common.base.Preconditions
import server.LogsActor
import akka.actor.ActorLogging
import com.xpatterns.jaws.data.contracts.DAL
import messages.GetDatabasesMessage
import java.util.UUID
import server.Configuration
import com.xpatterns.jaws.data.DTO.Result
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.scheduler.HiveUtils
import implementation.HiveContextWrapper
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage
import scala.util.Try
import com.xpatterns.jaws.data.DTO.Column
import com.xpatterns.jaws.data.DTO.Databases

/**
 * Created by emaorhian
 */
class GetDatabasesApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {

  override def receive = {

    case message: GetDatabasesMessage => {
      Configuration.log4j.info("[GetDatabasesApiActor]: showing databases")
      val currentSender = sender

      val getDatabasesFuture = future {
        val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
        val metadataQueryResult = HiveUtils.runMetadataCmd(hiveContext, "show databases").flatten
        new Databases(metadataQueryResult)
        
      }

      getDatabasesFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET databases failed with the following message: ${e.getMessage}")
      }
    }

  }
}
