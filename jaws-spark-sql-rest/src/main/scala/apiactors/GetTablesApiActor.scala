package apiactors

import messages.{ GetFormattedTablesMessage, GetTablesMessage, GetDatabasesMessage, GetExtendedTablesMessage }
import scala.concurrent.Await
import traits.DAL
import java.util.UUID
import akka.actor.Actor
import com.xpatterns.jaws.data.DTO.Result
import akka.util.Timeout
import server.Configuration
import scala.collection.mutable.Map
import akka.pattern.ask
import com.xpatterns.jaws.data.DTO.Tables
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.scheduler.HiveUtils
import implementation.HiveContextWrapper
import scala.collection.mutable.LinkedList

/**
 * Created by emaorhian
 */

trait DescriptionType
case class Extended() extends DescriptionType
case class Formatted() extends DescriptionType
case class Regular() extends DescriptionType

class GetTablesApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {
  val databasesActor = context.actorSelection("/user/GetDatabases")
  implicit val timeout = Timeout(Configuration.timeout)

  def getTablesForDatabase(database: String, isExtended: DescriptionType, describe: Boolean): Map[String, Result] = {
    var results = Map[String, Result]()
    Configuration.log4j.info("[GetTablesApiActor]: showing tables for database " + database)

    val useCommand = "use " + database
    val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()

    HiveUtils.runMetadataCmd(hiveContext, useCommand, dals.loggingDal, uuid)
    val tables = Result.trimResults(HiveUtils.runMetadataCmd(hiveContext, "show tables", dals.loggingDal, uuid))

    tables.results.foreach(result => {
      if (result.isEmpty == false) {
        if (describe) {
          results = results ++ describeTable(database, result(0), isExtended)
        } else {
          results.put(result(0), new Result)
        }
      }
    })

    results
  }

  def describeTables(database: String, tables: List[String]): Map[String, Result] = {
    var results = Map[String, Result]()
    Configuration.log4j.info(s"[describeTables]: describing the following tables from database= $database: $tables")
    tables.foreach(table => {
      results = results ++ describeTable(database, table, new Regular)
    })

    results
  }

  def describeTable(database: String, table: String, isExtended: DescriptionType): Map[String, Result] = {
    var results = Map[String, Result]()

    val useCommand = "use " + database
    val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
    HiveUtils.runMetadataCmd(hiveContext, useCommand, dals.loggingDal, uuid)

    Configuration.log4j.info("[GetTablesApiActor]: describing table " + table + " from database " + database)

    var cmd = ""
    isExtended match {
      case _: Extended => cmd = "describe extended " + table
      case _: Formatted => cmd = "describe formatted " + table
      case _ => cmd = "describe " + table
    }

    val description = Result.trimResults(HiveUtils.runMetadataCmd(hiveContext, cmd, dals.loggingDal, uuid))
    results.put(table, description)

    results
  }

  override def receive = {

    case message: GetTablesMessage => {

      var results = Map[String, Map[String, Result]]()

      // if no database is specified, the tables for all databases will be retrieved
      if (Option(message.database).getOrElse("").isEmpty) {
        val future = ask(databasesActor, GetDatabasesMessage())
        val allDatabases = Await.result(future, timeout.duration).asInstanceOf[Result]

        allDatabases.results.foreach(fields => results.put(fields(0), getTablesForDatabase(fields(0), new Regular, message.describe)))

      } else {
        // if there is a list of tables specified, then 
        if (Option(message.tables).getOrElse(new LinkedList()).isEmpty) {
          results.put(message.database, getTablesForDatabase(message.database, new Regular, message.describe))

        } else {
          results.put(message.database, describeTables(message.database, message.tables))
        }
      }

      sender ! Tables.fromMutableMap(results).tables

    }

    case message: GetExtendedTablesMessage => {
      val results = Map[String, Map[String, Result]]()

      if (Option(message.table).getOrElse("").isEmpty) {
        results.put(message.database, getTablesForDatabase(message.database, new Extended, true))

      } else {
        results.put(message.database, describeTable(message.database, message.table, new Extended))
      }

      sender ! Tables.fromMutableMap(results).tables

    }

    case message: GetFormattedTablesMessage => {
      val results = Map[String, Map[String, Result]]()

      if (Option(message.table).getOrElse("").isEmpty) {
        results.put(message.database, getTablesForDatabase(message.database, new Formatted, true))

      } else {
        results.put(message.database, describeTable(message.database, message.table, new Formatted))
      }

      sender ! Tables.fromMutableMap(results).tables

    }

  }
}