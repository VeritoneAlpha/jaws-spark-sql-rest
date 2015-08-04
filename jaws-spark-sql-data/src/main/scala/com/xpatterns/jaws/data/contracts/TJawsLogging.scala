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

  def setQueryName(queryId:String, name:String, description:String, overwrite:Boolean) = {
    Utils.TryWithRetry {
      val newQueryName = if (name != null) name.trim() else name

      if (!overwrite) {
        if (newQueryName != null && getQueriesByName(newQueryName).queries.nonEmpty) {
          // When the query name already exist and the overwrite flag is not set,
          // then the client should be warned about it
          throw new Exception(s"There is already a query with the name $name. To overwrite " +
            s"the query name, please send the parameter overwrite set on true")
        }
      } else if (newQueryName != null) {
        // When overwriting the old values, the old queries should have the name and description reset
        val notFoundState = QueryState.NOT_FOUND.toString
        for (query <- getQueriesByName(newQueryName).queries) {
          if (query.state != notFoundState) {
            query.metaInfo.name = null
            query.metaInfo.description = null
            setMetaInfo(query.queryID, query.metaInfo)
          }
        }
      }

      val metaInfo = getMetaInfo(queryId)
      if (metaInfo.name != null) {
        // delete the old query name
        deleteQueryName(metaInfo.name)
      }
      metaInfo.name = newQueryName
      metaInfo.description = description
      setMetaInfo(queryId, metaInfo)

      if (metaInfo.name != null) {
        // save the query name to be able to search it
        saveQueryName(metaInfo.name, queryId)
      }
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

  def getQueriesByName(name:String):Queries
  def deleteQueryName(name: String)
  def saveQueryName(name: String, queryId: String)

  def deleteQuery(queryId: String)
}