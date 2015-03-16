package apiactors

import messages._
import spray.http.StatusCodes
import scala.concurrent.Await
import traits.DAL
import java.util.UUID
import akka.util.Timeout
import server.Configuration
import akka.pattern.ask
import org.apache.spark.scheduler.HiveUtils
import implementation.HiveContextWrapper
import akka.actor.Actor
import com.xpatterns.jaws.data.DTO.{ Tables, Result }
import scala.util.{ Try, Success, Failure }
import apiactors.ActorOperations._
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.catalyst.types.StructField
import org.apache.spark.sql.catalyst.types.DataType
import com.xpatterns.jaws.data.DTO.Column
/**
 * Created by emaorhian
 */

class GetParquetTablesApiActor(hiveContext: HiveContextWrapper, dals: DAL) extends Actor {

  override def receive = {

    case message: GetParquetTablesMessage => {

      var results: Map[String, Map[String, Result]] = null

      val tryGetTables = Try {
        if (message.tables.isEmpty) {
          val tables = dals.parquetTableDal.listParquetTables
          message.describe match {
            case true => results = Map("None" -> (tables map (pTable => pTable.name -> getFields(pTable.name)) toMap))
            case false => results = Map("None" -> (tables map (pTable => pTable.name -> new Result) toMap))
          }

        } else {
          var tablesMap = message.tables.map(table => {
            if (dals.parquetTableDal.tableExists(table) == false)
              throw new Exception(s" Table $table does not exist")
            table -> getFields(table)
          }).toMap
          results = Map("None" -> tablesMap)
        }
      }

      returnResult(tryGetTables, results, "GET tables failed with the following message: ", sender)
    }
  }

  def getFields(tableName: String): Result = {
    val tableSchemaRDD = hiveContext.table(tableName)
    val schema = Array(Column("result", "StringType"))
    val result = tableSchemaRDD.schema

    new Result(schema, result)
  }

}