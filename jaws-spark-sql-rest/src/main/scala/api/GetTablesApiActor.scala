package api

import messages.GetTablesMessage
import scala.concurrent.Await
import traits.DAL
import java.util.UUID
import akka.actor.Actor
import com.xpatterns.jaws.data.DTO.Result
import akka.util.Timeout
import actors.Configuration
import scala.collection.mutable.Map
import akka.pattern.ask
import messages.GetDatabasesMessage
import messages.GetExtendedTablesMessage
import com.xpatterns.jaws.data.DTO.Tables
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.scheduler.HiveUtils

/**
 * Created by emaorhian
 */
class GetTablesApiActor(hiveContext: HiveContext, dals: DAL) extends Actor {
  val databasesActor = context.actorSelection("/user/GetDatabases")
  implicit val timeout = Timeout(Configuration.timeout)

  def getTablesForDatabase(database: String, isExtended: Boolean): Map[String, Result] = {
    var results = Map[String, Result]()
    Configuration.log4j.info("[GetTablesApiActor]: showing tables for database " + database)

    val useCommand = "use " + database
    val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()

    HiveUtils.runCmd(useCommand, hiveContext, uuid, dals.loggingDal)
    val tables = Result.trimResults(HiveUtils.runCmd("show tables", hiveContext, uuid, dals.loggingDal))
    tables.results.foreach(table => {
      results = results ++ getTableDescription(database, table(0), isExtended)
    })

    results
  }

  def getTableDescription(database: String, table: String, isExtended: Boolean): Map[String, Result] = {
    var results = Map[String, Result]()

    val useCommand = "use " + database
    val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
    HiveUtils.runCmd(useCommand, hiveContext, uuid, dals.loggingDal)

    Configuration.log4j.info("[GetTablesApiActor]: describing table " + table + " from database " + database)

    val cmd = if (isExtended) "describe extended " + table else "describe " + table
    val description = Result.trimResults(HiveUtils.runCmd(cmd, hiveContext, uuid, dals.loggingDal))
    results.put(table, description)

    results
  }

  override def receive = {

    case message: GetTablesMessage => {

      var results = Map[String, Map[String, Result]]()

      if (Option(message.database).getOrElse("").isEmpty) {

        val future = ask(databasesActor, GetDatabasesMessage())
        val allDatabases = Await.result(future, timeout.duration).asInstanceOf[Result]

        allDatabases.results.foreach(fields => results.put(fields(0), getTablesForDatabase(fields(0), false)))

      } else {
        results.put(message.database, getTablesForDatabase(message.database, false))
      }

      sender ! Tables.fromMutableMap(results).tables

    }

    case message: GetExtendedTablesMessage => {
      var results = Map[String, Map[String, Result]]()

      if (Option(message.table).getOrElse("").isEmpty) {
        results.put(message.database, getTablesForDatabase(message.database, true))

      } else {
        results.put(message.database, getTableDescription(message.database, message.table, true))
      }

      sender ! Tables.fromMutableMap(results).tables

    }

  }
}