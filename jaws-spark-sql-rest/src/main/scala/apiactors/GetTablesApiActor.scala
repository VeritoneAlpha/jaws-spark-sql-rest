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
import scala.util.{ Try, Success, Failure }
import apiactors.ActorOperations._
import scala.collection.immutable.HashMap
import scala.concurrent._
import ExecutionContext.Implicits.global
import messages.ErrorMessage
import com.xpatterns.jaws.data.DTO.Table
import com.xpatterns.jaws.data.DTO.Databases
import com.xpatterns.jaws.data.DTO.TableColumn
import com.xpatterns.jaws.data.DTO.Tables
import com.xpatterns.jaws.data.utils.Utils._
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

  def getTablesForDatabase(database: String, isExtended: DescriptionType, describe: Boolean): Tables = {
    Configuration.log4j.info(s"[GetTablesApiActor]: showing tables for database $database, describe = $describe")

    HiveUtils.runMetadataCmd(hiveContext, s"use $database")
    val tablesResult = HiveUtils.runMetadataCmd(hiveContext, "show tables")
    val tables = tablesResult map (arr => describe match {
      case true => describeTable(database, arr(0), isExtended)
      case _ => Table(arr(0), Array.empty, Array.empty)
    })

    print(s"!!!!!!!!!! the nr of tables in $database is ${tables.length}")
    Tables(database, tables)
  }

  def describeTable(database: String, table: String, isExtended: DescriptionType): Table = {
    Configuration.log4j.info(s"[GetTablesApiActor]: describing table $table from database $database")
    HiveUtils.runMetadataCmd(hiveContext, s"use $database")

    val cmd = isExtended match {
      case _: Extended => s"describe extended $table"
      case _: Formatted => s"describe formatted $table"
      case _ => s"describe $table"
    }

    val describedTable = HiveUtils.runMetadataCmd(hiveContext, cmd)
    
    println (s"!!!!!The number of columns is ${describedTable.size}")
    describedTable foreach (arr => {
      println("R1: ")
      arr.foreach(x => print(s"$x|"))
    })
    var x = new Table("",Array.empty,Array.empty)
    try{
    val columns = describedTable map (arr => {
      println(s"*********line ${arr(0)},   ${arr(1)},  ${arr(2)} |")
      TableColumn(arr(0), arr(1), arr(2))})
    
    println (s"!!!!!Table cols columns is $columns")
    columns foreach (col => {
      println(s"n : ${col.name} d : ${col.dataType}, c: ${col.comment} ")
    })
    
    x = Table(table, columns, Array.empty)
     println (s"!!!!!Table $x")
    
    }
    catch {
       
     case e: Exception => println(getCompleteStackTrace(e))
      
    }
    x
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
              case result: Databases => result.databases.map(db => getTablesForDatabase(db, new Regular, message.describe))
            }

          }
          case _ => {
            // if there is a list of tables specified, then
            if (Option(message.tables).getOrElse(Array.empty).isEmpty) {
              Array(getTablesForDatabase(message.database, new Regular, message.describe))

            } else {
              Array(Tables(message.database, message.tables map (table => describeTable(message.database, table, new Regular))))
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
         Option(message.tables).getOrElse(Array.empty).isEmpty match {
          case true => Array(getTablesForDatabase(message.database, new Extended, true))
          case _ => Array(Tables(message.database, message.tables map (table => describeTable(message.database, table, new Extended)))) 
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
         Option(message.tables).getOrElse(Array.empty).isEmpty match {
          case true => Array(getTablesForDatabase(message.database, new Formatted, true))
          case _ => Array(Tables(message.database, message.tables map (table => describeTable(message.database, table, new Formatted)))) 
        }
      }

      getFormattedTablesFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET formatted tables failed with the following message: ${e.getMessage}")
      }
    }

  }

}