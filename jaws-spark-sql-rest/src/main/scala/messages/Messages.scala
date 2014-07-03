package messages

/**
 * Created by emaorhian
 */
case class CancelMessage(uuid: String) extends Serializable
case class GetDatabasesMessage 
case class GetDescriptionMessage(uuid: String)
case class GetJobsMessage(startUuid: String, limit : Integer)
case class GetLogsMessage (uuid: String, startDate : Long, limit : Integer)
case class GetResultsMessage (uuid: String, offset : Integer, limit: Integer)
case class GetTablesMessage (database : String) 
case class RunScriptMessage(hqlScript : String, limited : Boolean, maxNumberOfResults: Long)