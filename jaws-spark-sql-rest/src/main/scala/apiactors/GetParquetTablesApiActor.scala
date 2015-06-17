package apiactors

import messages._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage
import spray.http.StatusCodes
import scala.concurrent.Await
import com.xpatterns.jaws.data.contracts.DAL
import java.util.UUID
import akka.util.Timeout
import server.Configuration
import akka.pattern.ask
import org.apache.spark.scheduler.HiveUtils
import implementation.HiveContextWrapper
import akka.actor.Actor
import com.xpatterns.jaws.data.DTO.Tables
import scala.util.{ Try, Success, Failure }
import apiactors.ActorOperations._
import com.xpatterns.jaws.data.DTO.Column
import com.xpatterns.jaws.data.DTO.Table
import com.xpatterns.jaws.data.utils.CustomConverter
/**
 * Created by emaorhian
 */

class GetParquetTablesApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {

  override def receive = {

    case message: GetParquetTablesMessage => {
      val currentSender = sender

      val getTablesFuture = future {
        if (message.tables.isEmpty) {
          val tables = dals.parquetTableDal.listParquetTables
          message.describe match {
            case true  => Array(Tables("None", tables map (pTable => getFields(pTable.name))))
            case false => Array(Tables("None", tables map (pTable => Table(pTable.name, Array.empty, Array.empty))))
          }

        } else {
          var tablesMap = message.tables.map(table => {
            if (dals.parquetTableDal.tableExists(table) == false)
              throw new Exception(s" Table $table does not exist")
             getFields(table)
          })
         Array(Tables("None", tablesMap))
        }
      }

      getTablesFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e)      => currentSender ! ErrorMessage(s"GET tables failed with the following message: ${e.getMessage}")
      }
    }
  }

  def getFields(tableName: String): Table = {
    val tableSchemaRDD = hiveContext.table(tableName)
    val schema = CustomConverter.getCustomSchema(tableSchemaRDD.schema)

    Table(tableName, schema, Array.empty)
  }

}