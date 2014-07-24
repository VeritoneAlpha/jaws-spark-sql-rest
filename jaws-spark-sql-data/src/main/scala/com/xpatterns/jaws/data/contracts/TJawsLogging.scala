package com.xpatterns.jaws.data.contracts

import com.xpatterns.jaws.data.utils.QueryState
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import com.xpatterns.jaws.data.DTO.Logs
import com.xpatterns.jaws.data.DTO.Queries
import com.xpatterns.jaws.data.utils.QueryState



/**
 * Created by emaorhian
 */
trait TJawsLogging {
  def setState(uuid: String, queryState: QueryState.QueryState)
  def setScriptDetails(uuid: String, scriptDetails: String)
  def addLog(uuid: String, queryId: String, time: Long, log: String)
  def setMetaInfo(uuid: String, metainfo: QueryMetaInfo)

  def getState(uuid: String): QueryState.QueryState
  def getScriptDetails(uuid: String): String
  def getLogs(uuid: String, time: Long, limit: Int): Logs
  def getMetaInfo(uuid: String): QueryMetaInfo

  def getQueriesStates(uuid: String, limit: Int): Queries
}