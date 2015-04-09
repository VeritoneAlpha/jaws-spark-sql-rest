package apiactors

import server.Configuration
import sys.process._
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
import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import scala.io.Source
import scala.io.BufferedSource

/**
 * Created by emaorhian
 */

case class RunQueryMessage(script: String, limit: Int)
case class ErrorMessage(message: String)

class HiveRunnerActor(dals: DAL) extends Actor {

  override def receive = {

    case message: RunQueryMessage => {
      Configuration.log4j.info(s"[HiveRunnerActor]: Running script=${message.script}")
      val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
      implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(Configuration.nrOfThreads.getOrElse("10").toInt))
      var script = ""

      val tryPreRunScript = Try {
        writeLaunchStatus(uuid, message.script)
        script = prepareCommands(message.script, message.limit)
      }

      tryPreRunScript match {
        case Success(v) => sender ! uuid
        case Failure(e) => sender ! ErrorMessage(s"Run hive query failed with the following message: ${e.getMessage}")
      }

      val runResponse = future {
        Configuration.log4j.info(s"[HiveRunnerActor]: Executing commands $script")
        runHiveScript(script, uuid)

      }

      runResponse onComplete {
        case Success(s) => {
          Configuration.log4j.info(s"[HiveRunnerActor]: Query $uuid has successfully finished")

        }
        case Failure(e) => {
          Configuration.log4j.info(s"[HiveRunnerActor]: Query $uuid has failed with the following exception ${e.getMessage()}")
        }
      }

    }
  }

  private def runHiveScript(script: String, uuid: String) = {
    val stdOutOS = new ByteArrayOutputStream
    val osWriter = new OutputStreamWriter(stdOutOS)

    val command = Seq("hive", "-e", script)

    try {
      command ! ProcessLogger(
        stdOutLine => osWriter.write(s"$stdOutLine\n"),
        stdErrLine => {
          Configuration.log4j.info(stdErrLine)
          dals.loggingDal.addLog(uuid, "hive", System.currentTimeMillis(), stdErrLine)
        })
      osWriter flush ()

      getLastResults(new ByteArrayInputStream(stdOutOS.toByteArray()))

    } finally {
      if (osWriter != null) osWriter close ()
    }
  }

  def getLastResults(inputStream: ByteArrayInputStream) = {
    val reader = Source.fromInputStream(inputStream)
    try {
      reader.getLines.foreach(line => println(line))
    } finally {
      if (reader != null) reader close ()
    }

  }
  private def writeLaunchStatus(uuid: String, script: String) {
    dals.loggingDal.addLog(uuid, "hive", System.currentTimeMillis(), s"Launching task for $uuid")
    dals.loggingDal.setState(uuid, QueryState.IN_PROGRESS)
    dals.loggingDal.setScriptDetails(uuid, script)
  }

}
