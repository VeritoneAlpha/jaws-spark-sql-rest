package messages

import implementation.SchemaSettingsFactory.{ StorageType, SourceType }

/**
 * Created by emaorhian
 */
case class CancelMessage(queryID: String) extends Serializable
case class GetDatabasesMessage()
case class GetQueriesMessage(queryIDs: Seq[String])
case class GetQueriesByName(name: String)
case class GetPaginatedQueriesMessage(startQueryID: String, limit: Int)
case class GetLogsMessage(queryID: String, startDate: Long, limit: Int)
case class GetResultsMessage(queryID: String, offset: Int, limit: Int, format : String)
case class GetTablesMessage(database: String, describe: Boolean, tables: Array[String])
case class GetExtendedTablesMessage(database: String, tables: Array[String])
case class GetFormattedTablesMessage(database: String, tables: Array[String])
case class RunQueryMessage(name: String)
case class RunScriptMessage(script: String, limited: Boolean, maxNumberOfResults: Long, rddDestination: String)
case class RunParquetMessage(script: String, tablePath: String, namenode:String, table: String, limited: Boolean, maxNumberOfResults: Long, rddDestination: String)
case class GetDatasourceSchemaMessage(path: String, sourceType: SourceType, storageType: StorageType)
case class ErrorMessage(message: String)
case class DeleteQueryMessage(queryID: String)
case class RegisterTableMessage(name: String, path: String, namenode: String)
case class UnregisterTableMessage(name: String)
case class GetParquetTablesMessage(tables: Array[String], describe: Boolean)
case class UpdateQueryNameMessage(queryID:String, name:String, description:String, overwrite:Boolean)


object ResultFormat {
  val AVRO_BINARY_FORMAT = "avrobinary"
  val AVRO_JSON_FORMAT = "avrojson"
  val DEFAULT_FORMAT = "default"
}
