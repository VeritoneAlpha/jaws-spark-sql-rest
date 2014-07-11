package messages

/**
 * Created by emaorhian
 */
case class CancelMessage(queryID: String) extends Serializable
case class GetDatabasesMessage 
case class GetQueryInfoMessage(queryID: String)
case class GetQueriesMessage(startQueryID: String, limit : Integer)
case class GetLogsMessage (queryID: String, startDate : Long, limit : Integer)
case class GetResultsMessage (queryID: String, offset : Integer, limit: Integer)
case class GetTablesMessage (database : String) 
case class RunScriptMessage(hqlScript : String, limited : Boolean, maxNumberOfResults: Long)