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

class RegisterParquetTableApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {
  override def receive = {

    case message: RegisterTableMessage => {
      Configuration.log4j.info(s"[RegisterParquetTableApiActor]: registering table ${message.name} at ${message.path}")

      val tryRegisterTable = Try {
        val (namenode, folderPath) = if (message.namenode.isEmpty()) HiveUtils.splitPath(message.path) else (message.namenode, message.path)
        HiveUtils.registerParquetTable(hiveContext, message.name, namenode, folderPath, dals)
      }

      returnResult(tryRegisterTable, s"Table ${message.name} was registered", "RegisterTable failed with the following message: ", sender)

    }

    case message: UnregisterTableMessage => {
      Configuration.log4j.info(s"[RegisterParquetTableApiActor]: Unregistering table ${message.name}")

      val tryUnregisterTable = Try {
        // unregister table
        hiveContext.getCatalog.unregisterTable(None, message.name)
        dals.parquetTableDal.deleteParquetTable(message.name)
      }

      returnResult(tryUnregisterTable, s"Table ${message.name} was unregistered", "UnregisterTable failed with the following message: ", sender)

    }
  }
}