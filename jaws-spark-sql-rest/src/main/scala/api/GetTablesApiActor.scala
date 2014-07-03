package api

import messages.GetTablesMessage
import scala.concurrent.Await
import traits.CustomSharkContext
import traits.DAL
import java.util.UUID
import akka.actor.Actor
import model.Result
import akka.util.Timeout
import model.Tables
import actors.Configuration
import scala.collection.mutable.Map
import akka.pattern.ask
import messages.GetDatabasesMessage
import org.apache.spark.scheduler.SharkUtils

/**
 * Created by emaorhian
 */
class GetTablesApiActor(customSharkContext: CustomSharkContext, dals: DAL) extends Actor {
  val databasesActor = context.actorSelection("/user/GetDatabases")
  implicit val timeout = Timeout(Configuration.timeout)

  def getTablesForDatabase(database: String): Map[String, Result] = {
    var results = Map[String, Result]()
    Configuration.log4j.info("[GetTablesApiActor]: showing tables for database " + database)

    val useCommand = "use " + database
    val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()

    SharkUtils.runCmd(useCommand, customSharkContext.sharkContext, uuid, dals.loggingDal)
    val tables = Result.trimResults(SharkUtils.runCmd("show tables", customSharkContext.sharkContext, uuid, dals.loggingDal))
    tables.getResults.foreach(table => {
      val description = Result.trimResults(SharkUtils.runCmd("describe " + table(0), customSharkContext.sharkContext, uuid, dals.loggingDal))
      results.put(table(0), description)
    })

    results
  }

  override def receive = {

    case message: GetTablesMessage => {

      var results = Map[String, Map[String, Result]]()

      if (Option(message.database).getOrElse("").isEmpty) {

        val future = ask(databasesActor, GetDatabasesMessage())
        val allDatabases = Await.result(future, timeout.duration).asInstanceOf[Result]

        allDatabases.getResults.foreach(fields => results.put(fields(0), getTablesForDatabase(fields(0))))

      } else {
        results.put(message.database, getTablesForDatabase(message.database))
      }

      sender ! Tables.fromMutableMap(results).tables

    }

  }
}