package com.xpatterns.jaws.data.contracts

import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import com.xpatterns.jaws.data.DTO.Logs
import com.xpatterns.jaws.data.DTO.Queries
import com.xpatterns.jaws.data.utils.QueryState
import com.xpatterns.jaws.data.utils.Utils
import com.xpatterns.jaws.data.DTO.Query

/**
 * Created by emaorhian
 */
trait TJawsLogging {
  def setState(queryId: String, queryState: QueryState.QueryState)
  def setScriptDetails(queryId: String, scriptDetails: String)
  def addLog(queryId: String, jobId: String, time: Long, log: String)

  def setExecutionTime(queryId: String, executionTime: Long): Unit = {
    Utils.TryWithRetry {
      val metaInfo = getMetaInfo(queryId)
      metaInfo.executionTime = executionTime
      setMetaInfo(queryId, metaInfo)
    }
  }

  def setTimestamp(queryId: String, time: Long): Unit = {
    Utils.TryWithRetry {
      val metaInfo = getMetaInfo(queryId)
      metaInfo.timestamp = time
      setMetaInfo(queryId, metaInfo)
    }
  }

  def setRunMetaInfo(queryId: String, metainfo: QueryMetaInfo) = {
    Utils.TryWithRetry {
      val newMetaInfo = getMetaInfo(queryId)
      newMetaInfo.nrOfResults = metainfo.nrOfResults
      newMetaInfo.maxNrOfResults = metainfo.maxNrOfResults
      newMetaInfo.resultsDestination = metainfo.resultsDestination
      newMetaInfo.isLimited = metainfo.isLimited
      setMetaInfo(queryId, newMetaInfo)
    }
  }

  def setMetaInfo(queryId: String, metainfo: QueryMetaInfo)

  def getState(queryId: String): QueryState.QueryState
  def getScriptDetails(queryId: String): String
  def getLogs(queryId: String, time: Long, limit: Int): Logs
  def getMetaInfo(queryId: String): QueryMetaInfo

  def getQueries(queryId: String, limit: Int): Queries
  def getQueries(queryIds: Seq[String]): Queries = {
    Utils.TryWithRetry {
      val queryArray = queryIds map (queryID => new Query(getState(queryID).toString, queryID, getScriptDetails(queryID), getMetaInfo(queryID))) toArray
      val queries = new Queries(queryArray)
      queries
    }
  }

  def deleteQuery(queryId: String)
}