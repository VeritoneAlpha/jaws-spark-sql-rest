package api

import messages.{GetFormattedTablesMessage, GetTablesMessage, GetDatabasesMessage, GetExtendedTablesMessage}
import scala.concurrent.Await
import traits.DAL
import java.util.UUID
import akka.actor.Actor
import com.xpatterns.jaws.data.DTO.Result
import akka.util.Timeout
import actors.Configuration
import scala.collection.mutable.Map
import akka.pattern.ask
import com.xpatterns.jaws.data.DTO.Tables
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.scheduler.HiveUtils
import implementation.HiveContextWrapper

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

  def getTablesForDatabase(database: String, isExtended: DescriptionType): Map[String, Result] = {
    var results = Map[String, Result]()
    Configuration.log4j.info("[GetTablesApiActor]: showing tables for database " + database)

    val useCommand = "use " + database
    val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()

    HiveUtils.runMetadataCmd(hiveContext, useCommand, dals.loggingDal, uuid)
    val tables = Result.trimResults(HiveUtils.runMetadataCmd(hiveContext, "show tables", dals.loggingDal, uuid))
    tables.results.foreach(result => {
      if (result.isEmpty == false){
        results = results ++ getTableDescription(database, result(0), isExtended)
      }
    })

    results
  }

  def getTableDescription(database: String, table: String, isExtended: DescriptionType): Map[String, Result] = {
    var results = Map[String, Result]()

    val useCommand = "use " + database
    val uuid = System.currentTimeMillis() + UUID.randomUUID().toString()
    HiveUtils.runMetadataCmd(hiveContext, useCommand, dals.loggingDal, uuid)

    Configuration.log4j.info("[GetTablesApiActor]: describing table " + table + " from database " + database)

    var cmd = ""
    isExtended match {
      case _ : Extended => cmd = "describe extended " + table
      case _ : Formatted => cmd = "describe formatted " + table
      case _ => cmd = "describe " + table
    }

    val description = Result.trimResults(HiveUtils.runMetadataCmd(hiveContext, cmd, dals.loggingDal, uuid))
    results.put(table, description)

    results
  }

  override def receive = {

    case message: GetTablesMessage => {

      var results = Map[String, Map[String, Result]]()

      if (Option(message.database).getOrElse("").isEmpty) {

        val future = ask(databasesActor, GetDatabasesMessage())
        val allDatabases = Await.result(future, timeout.duration).asInstanceOf[Result]

        allDatabases.results.foreach(fields => results.put(fields(0), getTablesForDatabase(fields(0), new Regular)))


      } else {
        results.put(message.database, getTablesForDatabase(message.database, new Regular))
      }

      sender ! Tables.fromMutableMap(results).tables

    }

    case message: GetExtendedTablesMessage => {
      val results = Map[String, Map[String, Result]]()

      if (Option(message.table).getOrElse("").isEmpty) {
        results.put(message.database, getTablesForDatabase(message.database, new Extended))

      } else {
        results.put(message.database, getTableDescription(message.database, message.table, new Extended))
      }

      sender ! Tables.fromMutableMap(results).tables

    }

    case message: GetFormattedTablesMessage => {
      val results = Map[String, Map[String, Result]]()

      if (Option(message.table).getOrElse("").isEmpty) {
        results.put(message.database, getTablesForDatabase(message.database, new Formatted))

      } else {
        results.put(message.database, getTableDescription(message.database, message.table, new Formatted))
      }

      sender ! Tables.fromMutableMap(results).tables

    }

  }
}