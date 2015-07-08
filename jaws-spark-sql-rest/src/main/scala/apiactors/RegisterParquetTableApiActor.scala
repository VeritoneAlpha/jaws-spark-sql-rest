package apiactors

import implementation.HiveContextWrapper
import com.xpatterns.jaws.data.contracts.DAL
import akka.actor.Actor
import messages.RegisterTableMessage
import server.Configuration
import scala.util.Try
import apiactors.ActorOperations._
import org.apache.spark.scheduler.HiveUtils
import messages.UnregisterTableMessage
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage

class RegisterParquetTableApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {
  override def receive = {

    case message: RegisterTableMessage => {
      Configuration.log4j.info(s"[RegisterParquetTableApiActor]: registering table ${message.name} at ${message.path}")
      val currentSender = sender

      val registerTableFuture = future {
        val (namenode, folderPath) = if (message.namenode.isEmpty) HiveUtils.splitPath(message.path) else (message.namenode, message.path)
        HiveUtils.registerParquetTable(hiveContext, message.name, namenode, folderPath, dals)
      }

      registerTableFuture onComplete {
        case Success(_) => currentSender ! s"Table ${message.name} was registered"
        case Failure(e) => currentSender ! ErrorMessage(s"RegisterTable failed with the following message: ${e.getMessage}")
      }
    }

    case message: UnregisterTableMessage => {
      Configuration.log4j.info(s"[RegisterParquetTableApiActor]: Unregistering table ${message.name}")
      val currentSender = sender

      val unregisterTableFuture = future {
        // unregister table
    	hiveContext.getCatalog.unregisterTable(Seq(message.name))
        dals.parquetTableDal.deleteParquetTable(message.name)
      }

      unregisterTableFuture onComplete {
        case Success(result) => currentSender ! s"Table ${message.name} was unregistered"
        case Failure(e) => currentSender ! ErrorMessage(s"UnregisterTable failed with the following message: ${e.getMessage}")
      }
    }
  }
}