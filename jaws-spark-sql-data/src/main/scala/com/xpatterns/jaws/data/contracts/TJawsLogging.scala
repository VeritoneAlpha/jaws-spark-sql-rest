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

  def setQueryProperties(queryId: String, name: Option[String], description: Option[String], published:Option[Boolean],
                         overwrite: Boolean) = {
    Utils.TryWithRetry {
      val metaInfo = getMetaInfo(queryId)

      if (name != None) {
        updateQueryName(queryId, metaInfo, name.get, overwrite)
      }

      if (description != None) {
        metaInfo.description = description
      }

      // When the name of a query is not present, the description and published flags should be removed,
      // because they appear only when a query has a name
      if (metaInfo.name == None || metaInfo.name.get == null) {
        metaInfo.description = None
        metaInfo.published = None
      } else if (published != None) {
        setQueryPublishedStatus(metaInfo.name.get, metaInfo, published.get)
        metaInfo.published = published
      }

      setMetaInfo(queryId, metaInfo)
    }
  }

  private def updateQueryName(queryId: String, metaInfo: QueryMetaInfo, name: String, overwrite:Boolean):Unit = {
    val newQueryName = if (name != null) name.trim() else null

    if (newQueryName != null && newQueryName.isEmpty) {
      return
    }

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
          query.metaInfo.name = None
          query.metaInfo.description = None
          setMetaInfo(query.queryID, query.metaInfo)
        }
      }
    }

    if (metaInfo.name != None && metaInfo.name.get != null) {
      // Delete the old query name
      deleteQueryName(metaInfo.name.get)
      // Remove the old published status of the query from storage
      deleteQueryPublishedStatus(metaInfo.name.get, metaInfo.published)
    }
    metaInfo.name = Some(newQueryName)

    if (newQueryName != null) {
      // Save the query name to be able to search it
      saveQueryName(newQueryName, queryId)

      // Set the default published value
      val published = metaInfo.published.getOrElse(false)
      setQueryPublishedStatus(newQueryName, metaInfo, published)
      metaInfo.published = Some(published)
    }
  }

  def setQueryPublishedStatus(name: String, metaInfo: QueryMetaInfo, published: Boolean)
  def deleteQueryPublishedStatus(name: String, published: Option[Boolean])

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

  def getPublishedQueries():Array[String]
  def getQueriesByName(name:String):Queries
  def deleteQueryName(name: String)
  def saveQueryName(name: String, queryId: String)

  def deleteQuery(queryId: String)
}