package messages

import implementation.SchemaSettingsFactory.{ StorageType, SourceType }

/**
 * Created by emaorhian
 */
case class CancelMessage(queryID: String) extends Serializable
case class GetDatabasesMessage()
case class GetQueryInfoMessage(queryID: String)
case class GetQueriesMessage(startQueryID: String, limit: Integer)
case class GetLogsMessage(queryID: String, startDate: Long, limit: Integer)
case class GetResultsMessage(queryID: String, offset: Integer, limit: Integer)
case class GetTablesMessage(database: String, describe: Boolean, tables: List[String])
case class GetExtendedTablesMessage(database: String, table: String)
case class GetFormattedTablesMessage(database: String, table: String)
case class RunScriptMessage(script: String, limited: Boolean, maxNumberOfResults: Long, rddDestination: String)
case class RunParquetMessage(script: String, tablePath: String, table: String, limited: Boolean, maxNumberOfResults: Long, rddDestination: String)
case class GetDatasourceSchemaMessage(path: String, sourceType: SourceType, storageType: StorageType)
case class ErrorMessage(message: String)
case class DeleteQueryMessage(queryID: String)
case class RegisterTableMessage(name: String, path: String, namenode: String = "") 
case class UnregisterTableMessage(name: String)
case class GetParquetTablesMessage(tables: List[String], describe: Boolean)
