package apiactors

import server.Configuration
import scala.collection.mutable.ListBuffer
import akka.actor.Actor
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import com.xpatterns.jaws.data.contracts.DAL
import customs.CommandsProcessor._
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import sys.process._
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import java.util.UUID
import com.xpatterns.jaws.data.utils.QueryState
import scala.concurrent._

/**
 * Created by emaorhian
 */

case class RunQueryMessage(script: String, limit: Int)
case class ErrorMessage(message: String)

class HiveRunnerActor(dals: DAL) extends Actor {

  override def receive = {

    case message: RunQueryMessage => {
      Configuration.log4j.info(s"[HiveRunnerActor]: Running script=${message.script}")
      val stdOutBaos = new ByteArrayOutputStream
      val osWriter = new OutputStreamWriter(stdOutBaos)
      val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
      implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(Configuration.nrOfThreads.getOrElse("10").toInt))

      val tryRunScript = Try {
        writeLaunchStatus(uuid, message.script)
        val command = Seq("hive", "-e", prepareCommands(message.script))
        val runResponse = future {
          print ("do something")
        }
        sender ! uuid
        
        runResponse onComplete(null)
      }

      tryRunScript match {
        case Success(v) => sender ! tryRunScript.get
        case Failure(e) => sender ! ErrorMessage(s"Run hive query failed with the following message: ${e.getMessage}")
      }
    }
  }
  
  private def writeLaunchStatus(uuid: String, script: String) {
    dals.loggingDal.addLog(uuid, "hive", System.currentTimeMillis(), s"Launching task for $uuid")
    dals.loggingDal.setState(uuid, QueryState.IN_PROGRESS)
    dals.loggingDal.setScriptDetails(uuid, script)
  }


}
