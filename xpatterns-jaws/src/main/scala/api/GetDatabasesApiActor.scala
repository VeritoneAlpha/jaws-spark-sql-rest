package api

import akka.actor.Actor
import akka.actor.actorRef2Scala
import messages.GetJobsMessage
import com.google.common.base.Preconditions
import actors.LogsActor
import akka.actor.ActorLogging
import traits.DAL
import model.Jobs
import model.Job
import messages.GetDescriptionMessage
import messages.GetDatabasesMessage
import java.util.UUID
import traits.CustomSharkContext
import actors.Configuration
import model.Result
import org.apache.spark.scheduler.SharkUtils
/**
 * Created by emaorhian
 */
class GetDatabasesApiActor(customSharkContext: CustomSharkContext, dals: DAL) extends Actor{

  override def receive = {

    case message: GetDatabasesMessage => {
      Configuration.log4j.info("[GetDatabasesApiActor]: showing databases")
      val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
      val databases = SharkUtils.runCmd("show databases", customSharkContext.sharkContext, uuid, dals.loggingDal)

      sender ! Result.trimResults(databases)
    }
  }
}
