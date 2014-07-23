package com.xpatterns.jaws.data.contracts

import com.xpatterns.jaws.data.utils.QueryState
import java.util.Collection

trait TJawsLogging {
	def setState(uuid : String, queryState : QueryState)
	def setScriptDetails(uuid : String, scriptDetails: String)
	def addLog(uuid : String, queryId : String , time : Long, log : String)
	def setMetaInfo(String uuid, ScriptMetaDTO metainfo) throws Exception;
	
	public QueryState getState(String uuid) throws IOException;
	public String getScriptDetails(String uuid) throws IOException;
	public Collection<LogDTO> getLogs(String uuid, Long time, int limit) throws IOException;
	public ScriptMetaDTO getMetaInfo(String uuid) throws IOException;
	
	Collection<StateDTO> getQueriesStates(String uuid, int limit) throws IOException;
}