package apiactors

import messages._
import spray.http.StatusCodes
import scala.concurrent.Await
import com.xpatterns.jaws.data.contracts.DAL
import java.util.UUID
import akka.util.Timeout
import server.Configuration
import scala.collection.immutable.Map
import akka.pattern.ask
import org.apache.spark.scheduler.HiveUtils
import implementation.HiveContextWrapper
import akka.actor.Actor
import com.xpatterns.jaws.data.DTO.{ Tables, Result }
import scala.util.{ Try, Success, Failure }
import apiactors.ActorOperations._
import scala.collection.immutable.HashMap
import scala.concurrent._
import ExecutionContext.Implicits.global
import messages.ErrorMessage

/**
 * Created by emaorhian
 */

trait DescriptionType
case class Extended() extends DescriptionType
case class Formatted() extends DescriptionType
case class Regular() extends DescriptionType

class GetTablesApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {

  val databasesActor = context.actorSelection(ActorsPaths.GET_DATABASES_ACTOR_PATH)
  implicit val timeout = Timeout(Configuration.timeout)

  def getTablesForDatabase(database: String, isExtended: DescriptionType, describe: Boolean): Map[String, Result] = {
    Configuration.log4j.info("[GetTablesApiActor]: showing tables for database " + database)

    HiveUtils.runMetadataCmd(hiveContext, s"use $database")
    val tables = Result.trimResults(HiveUtils.runMetadataCmd(hiveContext, "show tables"))

    tables.results flatMap (result => {
      if (result.isEmpty == false) {
        if (describe) describeTable(database, result(0), isExtended) else Map(result(0) -> new Result)
      } else {
        Map[String, Result]()
      }
    }) toMap
  }

  def describeTables(database: String, tables: List[String]): Map[String, Result] = {
    Configuration.log4j.info(s"[describeTables]: describing the following tables from database= $database: $tables")
    tables flatMap (table => describeTable(database, table, new Regular)) toMap
  }

  def describeTable(database: String, table: String, isExtended: DescriptionType): Map[String, Result] = {
    Configuration.log4j.info(s"[GetTablesApiActor]: describing table $table from database $database")
    HiveUtils.runMetadataCmd(hiveContext, s"use $database")

    val cmd = isExtended match {
      case _: Extended => s"describe extended $table"
      case _: Formatted => s"describe formatted $table"
      case _ => s"describe $table"
    }

    val description = Result.trimResults(HiveUtils.runMetadataCmd(hiveContext, cmd))
    Map(table -> description)
  }

  override def receive = {

    case message: GetTablesMessage => {
      val currentSender = sender

      val getTablesFutures = future {
        // if no database is specified, the tables for all databases will be retrieved
        Option(message.database).getOrElse("") match {
          case "" => {
            val future = ask(databasesActor, GetDatabasesMessage())
            val allDatabases = Await.result(future, timeout.duration)

            allDatabases match {
              case e: ErrorMessage => throw new Exception(e.message)
              case result: Result => result.results.flatMap(fields => Map(fields(0) -> getTablesForDatabase(fields(0), new Regular, message.describe))) toMap
            }

          }
          case _ => {
            // if there is a list of tables specified, then
            if (Option(message.tables).getOrElse(List()).isEmpty) {
              Map(message.database -> getTablesForDatabase(message.database, new Regular, message.describe))

            } else {
              Map(message.database -> describeTables(message.database, message.tables))
            }
          }
        }
      }

      getTablesFutures onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET tables failed with the following message: ${e.getMessage}")
      }
    }

    case message: GetExtendedTablesMessage => {
      val currentSender = sender
      val getExtendedTablesFuture = future {
        Option(message.table).getOrElse("") match {
          case "" => Map(message.database -> getTablesForDatabase(message.database, new Extended, true))
          case _ => Map(message.database -> describeTable(message.database, message.table, new Extended))
        }
      }

      getExtendedTablesFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET extended tables failed with the following message: ${e.getMessage}")
      }
    }

    case message: GetFormattedTablesMessage => {
      val currentSender = sender

      val getFormattedTablesFuture = future {
        Option(message.table).getOrElse("") match {
          case "" => Map(message.database -> getTablesForDatabase(message.database, new Formatted, true))
          case _ => Map(message.database -> describeTable(message.database, message.table, new Formatted))
        }
      }

      getFormattedTablesFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET formatted tables failed with the following message: ${e.getMessage}")
      }
    }

  }

}