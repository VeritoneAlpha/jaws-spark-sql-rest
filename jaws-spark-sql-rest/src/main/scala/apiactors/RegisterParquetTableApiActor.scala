package apiactors

import implementation.HiveContextWrapper
import traits.DAL
import akka.actor.Actor
import messages.RegisterTableMessage
import server.Configuration
import scala.util.Try
import apiactors.ActorOperations._
import org.apache.spark.scheduler.HiveUtils

class RegisterParquetTableApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {
  override def receive = {

    case message: RegisterTableMessage => {
      Configuration.log4j.info(s"[RegisterParquetTableApiActor]: registering table ${message.name} at ${message.path}")

      val tryRegisterTable = Try {
        val (namenode, folderPath) = HiveUtils.splitPath(message.path)
        HiveUtils.registerParquetTable(hiveContext, message.name, namenode, folderPath, dals)
      }

      returnResult(tryRegisterTable, s"Table ${message.name} was registered", "RegisterTable failed with the following message: ", sender)

    }
  }
}