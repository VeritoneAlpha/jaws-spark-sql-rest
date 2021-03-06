package messages

import implementation.SchemaSettingsFactory.{ StorageType, SourceType }
import org.apache.hadoop.conf.Configuration

/**
 * Created by emaorhian
 */
case class CancelMessage(queryID: String) extends Serializable
case class GetDatabasesMessage()
case class GetQueriesMessage(queryIDs: Seq[String])
case class GetQueriesByName(name: String)
case class GetPublishedQueries()
case class GetPaginatedQueriesMessage(startQueryID: String, limit: Int)
case class GetLogsMessage(queryID: String, startDate: Long, limit: Int)
case class GetResultsMessage(queryID: String, offset: Int, limit: Int, format : String)
case class GetTablesMessage(database: String, describe: Boolean, tables: Array[String])
case class GetExtendedTablesMessage(database: String, tables: Array[String])
case class GetFormattedTablesMessage(database: String, tables: Array[String])
case class RunQueryMessage(name: String)
case class RunScriptMessage(script: String, limited: Boolean, maxNumberOfResults: Long, rddDestination: String)
case class RunParquetMessage(script: String, tablePath: String, namenode:String, table: String, limited: Boolean, maxNumberOfResults: Long, rddDestination: String)
case class GetDatasourceSchemaMessage(path: String, sourceType: SourceType, storageType: StorageType, hdfsConf:Configuration)
case class ErrorMessage(message: String)
case class DeleteQueryMessage(queryID: String)
case class RegisterTableMessage(name: String, path: String, namenode: String)
case class UnregisterTableMessage(name: String)
case class GetParquetTablesMessage(tables: Array[String], describe: Boolean)
case class UpdateQueryPropertiesMessage(queryID:String, name:Option[String], description:Option[String], published:Option[Boolean], overwrite:Boolean)


object ResultFormat {
  val AVRO_BINARY_FORMAT = "avrobinary"
  val AVRO_JSON_FORMAT = "avrojson"
  val DEFAULT_FORMAT = "default"
}
