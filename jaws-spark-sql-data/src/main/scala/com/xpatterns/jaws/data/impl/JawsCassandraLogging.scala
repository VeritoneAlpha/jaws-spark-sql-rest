package com.xpatterns.jaws.data.impl

import java.util.TreeMap
import scala.collection.JavaConverters._
import org.apache.log4j.Logger
import com.xpatterns.jaws.data.DTO.Log
import com.xpatterns.jaws.data.DTO.Logs
import com.xpatterns.jaws.data.DTO.Queries
import com.xpatterns.jaws.data.DTO.Query
import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.utils.{ Utils, QueryState }
import me.prettyprint.cassandra.serializers.CompositeSerializer
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.cassandra.serializers.LongSerializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality
import me.prettyprint.hector.api.beans.ColumnSlice
import me.prettyprint.hector.api.beans.Composite
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.mutation.Mutator
import me.prettyprint.hector.api.query.QueryResult
import me.prettyprint.hector.api.query.SliceQuery
import net.liftweb.json._
import spray.json._
import spray.json.DefaultJsonProtocol._
import me.prettyprint.hector.api.query.MultigetSliceQuery
import me.prettyprint.hector.api.beans.Rows
import java.util.ArrayList
import com.xpatterns.jaws.data.DTO.QueryMetaInfo

class JawsCassandraLogging(keyspace: Keyspace) extends TJawsLogging {

  val CF_SPARK_LOGS = "logs"
  val CF_SPARK_LOGS_NUMBER_OF_ROWS = 100

  val LEVEL_TYPE = 0
  val LEVEL_UUID = 1
  val LEVEL_TIME_STAMP = 2

  val TYPE_QUERY_STATE = -1
  val TYPE_SCRIPT_DETAILS = 0
  val TYPE_LOG = 1
  val TYPE_META = 2

  val logger = Logger.getLogger("JawsCassandraLogging")

  val is = IntegerSerializer.get.asInstanceOf[Serializer[Int]]
  val ss = StringSerializer.get.asInstanceOf[Serializer[String]]
  val cs = CompositeSerializer.get.asInstanceOf[Serializer[Composite]]
  val ls = LongSerializer.get.asInstanceOf[Serializer[Long]]

  override def setState(queryId: String, queryState: QueryState.QueryState) {
    Utils.TryWithRetry {

      logger.debug("Writing query state " + queryState.toString() + " to query " + queryId)

      val key = computeRowKey(queryId)

      val column = new Composite()
      column.setComponent(LEVEL_UUID, queryId, ss)
      column.setComponent(LEVEL_TYPE, TYPE_QUERY_STATE, is)

      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, queryState.toString(), cs, ss))
      mutator.execute()
    }
  }

  override def setScriptDetails(queryId: String, scriptDetails: String) {
    Utils.TryWithRetry {

      logger.debug("Writing script details " + scriptDetails + " to query " + queryId)

      val key = computeRowKey(queryId)

      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_SCRIPT_DETAILS, is)
      column.setComponent(LEVEL_UUID, queryId, ss)

      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, scriptDetails, cs, ss))
      mutator.execute()
    }
  }

  private def computeRowKey(uuid: String): Integer = {
    Math.abs(uuid.hashCode() % CF_SPARK_LOGS_NUMBER_OF_ROWS)
  }

  override def addLog(queryId: String, jobId: String, time: Long, log: String) {
    Utils.TryWithRetry {

      logger.debug("Writing log " + log + " to query " + queryId + " at time " + time)
      val dto = new Log(log, jobId, time)

      val key = computeRowKey(queryId)

      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_LOG, is)
      column.setComponent(LEVEL_UUID, queryId, ss)
      column.setComponent(LEVEL_TIME_STAMP, time, ls)

      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, dto.toJson.toString(), cs, StringSerializer.get.asInstanceOf[Serializer[Object]]))
      mutator.execute()
    }
  }

  override def getState(queryId: String): QueryState.QueryState = {
    Utils.TryWithRetry {

      logger.debug("Reading query state for query: " + queryId)

      val key = computeRowKey(queryId)

      val column = new Composite()
      column.addComponent(LEVEL_TYPE, TYPE_QUERY_STATE, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_UUID, queryId, ComponentEquality.EQUAL)

      val sliceQuery: SliceQuery[Int, Composite, String] = HFactory.createSliceQuery(keyspace, is, cs, ss)
      sliceQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setRange(column, column, false, 1)

      val result: QueryResult[ColumnSlice[Composite, String]] = sliceQuery.execute()

      Option(result) match {
        case None => logger.info("No results found")
        case _ => {

          val columnSlice: ColumnSlice[Composite, String] = result.get()

          Option(columnSlice) match {
            case None => return QueryState.NOT_FOUND
            case _ => {
              Option(columnSlice.getColumns()) match {
                case None => return QueryState.NOT_FOUND
                case _ => {
                  if (columnSlice.getColumns().size() == 0) {
                    return QueryState.NOT_FOUND
                  }
                  val col = columnSlice.getColumns().get(0)
                  val state = col.getValue()

                  return QueryState.withName(state)

                }
              }
            }
          }

        }
      }

      return QueryState.NOT_FOUND
    }
  }

  override def getScriptDetails(queryId: String): String = {
    Utils.TryWithRetry {

      logger.debug("Reading script details for query: " + queryId)

      val key = computeRowKey(queryId)

      val column = new Composite()
      column.addComponent(LEVEL_TYPE, TYPE_SCRIPT_DETAILS, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_UUID, queryId, ComponentEquality.EQUAL)

      val sliceQuery: SliceQuery[Int, Composite, String] = HFactory.createSliceQuery(keyspace, is, cs, ss)
      sliceQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setRange(column, column, false, 1)

      val result: QueryResult[ColumnSlice[Composite, String]] = sliceQuery.execute()
      Option(result) match {
        case None => return ""
        case _ => {
          val columnSlice: ColumnSlice[Composite, String] = result.get()
          Option(columnSlice) match {
            case None => return ""
            case _ => {
              Option(columnSlice.getColumns()) match {
                case None => return ""
                case _ => {

                  if (columnSlice.getColumns().size() == 0) {
                    return ""
                  }
                  val col: HColumn[Composite, String] = columnSlice.getColumns().get(0)
                  val description = col.getValue()

                  return description
                }
              }

            }
          }

        }
      }
    }
  }

  override def getLogs(queryId: String, time: Long, limit: Int): Logs = {
    Utils.TryWithRetry {

      logger.debug("Reading logs for query: " + queryId + " from date: " + time)
      var logs = Array[Log]()
      val state = getState(queryId).toString
      val key = computeRowKey(queryId)

      val startColumn = new Composite()
      startColumn.addComponent(LEVEL_TYPE, TYPE_LOG, ComponentEquality.EQUAL)
      startColumn.addComponent(LEVEL_UUID, queryId, ComponentEquality.EQUAL)
      startColumn.addComponent(LEVEL_TIME_STAMP, time, ComponentEquality.EQUAL)

      val endColumn = new Composite()
      endColumn.addComponent(LEVEL_TYPE, TYPE_LOG, ComponentEquality.EQUAL)
      endColumn.addComponent(LEVEL_UUID, queryId, ComponentEquality.GREATER_THAN_EQUAL)
      val sliceQuery: SliceQuery[Int, Composite, String] = HFactory.createSliceQuery(keyspace, is, cs, ss)
      sliceQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setRange(startColumn, endColumn, false, limit)

      val result: QueryResult[ColumnSlice[Composite, String]] = sliceQuery.execute()

      Option(result) match {
        case None => return new Logs(logs, getState(queryId).toString)
        case _ => {
          val columnSlice: ColumnSlice[Composite, String] = result.get()
          Option(columnSlice) match {
            case None => return new Logs(logs, getState(queryId).toString)
            case _ => {
              Option(columnSlice.getColumns()) match {

                case None => return new Logs(logs, getState(queryId).toString)
                case _ => {
                  if (columnSlice.getColumns().size() == 0) {
                    return new Logs(logs, getState(queryId).toString)
                  }

                  val columns = columnSlice.getColumns().asScala
                  implicit val formats = DefaultFormats
                  columns.foreach(col => {
                    val value = col.getValue()
                    val json = parse(value)
                    val log = json.extract[Log]
                    logs = logs ++ Array(log)
                  })
                  return return new Logs(logs, state)
                }
              }

            }
          }

        }
      }
    }
  }

  override def getQueriesStates(queryId: String, limit: Int): Queries = {
    Utils.TryWithRetry {

      var skipFirst = false
      logger.debug("Reading queries states starting with the query: " + queryId)

      val map = new TreeMap[String, Query]()
      val stateList = Array[Query]()
      val keysList: java.util.List[Integer] = new ArrayList[Integer]()

      for (key <- 0 until CF_SPARK_LOGS_NUMBER_OF_ROWS) {
        keysList.add(key)
      }

      val startColumn = new Composite()
      startColumn.addComponent(LEVEL_TYPE, TYPE_QUERY_STATE, ComponentEquality.EQUAL)
      if (queryId != null && !queryId.isEmpty()) {
        startColumn.addComponent(LEVEL_UUID, queryId, ComponentEquality.EQUAL)
      }

      val endColumn = new Composite()
      if (queryId != null && !queryId.isEmpty()) {
        endColumn.addComponent(LEVEL_TYPE, TYPE_QUERY_STATE, ComponentEquality.LESS_THAN_EQUAL)
      } else {
        endColumn.addComponent(LEVEL_TYPE, TYPE_QUERY_STATE, ComponentEquality.GREATER_THAN_EQUAL)
      }

      val multiSliceQuery: MultigetSliceQuery[Integer, Composite, String] = HFactory.createMultigetSliceQuery(keyspace, IntegerSerializer.get.asInstanceOf[Serializer[Integer]], cs, ss)
      if (queryId != null && !queryId.isEmpty()) {
        multiSliceQuery.setColumnFamily(CF_SPARK_LOGS).setKeys(keysList).setRange(startColumn, endColumn, true, limit + 1)
        skipFirst = true
      } else {
        multiSliceQuery.setColumnFamily(CF_SPARK_LOGS).setKeys(keysList).setRange(endColumn, startColumn, true, limit)
      }

      val result: QueryResult[Rows[Integer, Composite, String]] = multiSliceQuery.execute()
      val rows = result.get()
      if (rows == null || rows.getCount() == 0) {
        return new Queries(stateList)
      }

      val rrows = rows.asScala

      rrows.foreach(row => {

        val columnSlice = row.getColumnSlice()
        if (columnSlice == null || columnSlice.getColumns() == null || columnSlice.getColumns().size() == 0) {

        } else {
          val columns = columnSlice.getColumns().asScala
          columns.foreach(column => {
            val name = column.getName
            if (name.get(LEVEL_TYPE, is) == TYPE_QUERY_STATE) {
              val queryId = name.get(LEVEL_UUID, ss)
              val query = new Query(column.getValue(), queryId, getScriptDetails(queryId), getMetaInfo(queryId))
              map.put(name.get(LEVEL_UUID, ss), query)
            }
          })
        }
      })

      return Queries(getCollectionFromSortedMapWithLimit(map, limit, skipFirst))
    }
  }

  def getCollectionFromSortedMapWithLimit(map: TreeMap[String, Query], limit: Int, skipFirst: Boolean): Array[Query] = {

    var collection = Array[Query]()
    val iterator = map.descendingKeySet().iterator()
    var skipFirstMutable = skipFirst
    var limitMutable = limit

    while (iterator.hasNext() && limitMutable > 0) {
      if (skipFirstMutable) {
        skipFirstMutable = false
        iterator.next()
      } else {
        collection = collection ++ Array(map.get(iterator.next()))
        limitMutable = limitMutable - 1
      }
    }

    return collection
  }

  override def setMetaInfo(queryId: String, metainfo: QueryMetaInfo) {
    Utils.TryWithRetry {

      logger.debug("Writing script meta info " + metainfo + " to query " + queryId)

      val key = computeRowKey(queryId)

      val column = new Composite()
      column.setComponent(LEVEL_TYPE, TYPE_META, is)
      column.setComponent(LEVEL_UUID, queryId, ss)

      val value = metainfo.toJson.toString

      val mutator = HFactory.createMutator(keyspace, is)

      mutator.addInsertion(key, CF_SPARK_LOGS, HFactory.createColumn(column, value, cs, StringSerializer.get.asInstanceOf[Serializer[Object]]))
      mutator.execute()
    }
  }

  override def getMetaInfo(queryId: String): QueryMetaInfo = {
    Utils.TryWithRetry {

      logger.debug("Reading meta info for for query: " + queryId)

      val key = computeRowKey(queryId)

      val column = new Composite()
      column.addComponent(LEVEL_TYPE, TYPE_META, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_UUID, queryId, ComponentEquality.EQUAL)

      val columnQuery = HFactory.createColumnQuery(keyspace, is, cs, ss)
      columnQuery.setColumnFamily(CF_SPARK_LOGS).setKey(key).setName(column)

      val result = columnQuery.execute()
      if (result != null) {
        val col = result.get()

        if (col == null) {
          return new QueryMetaInfo()
        }
        implicit val formats = DefaultFormats
        val value = col.getValue()
        val json = parse(value)
        return json.extract[QueryMetaInfo]
      }

      return new QueryMetaInfo()
    }
  }

  def deleteQuery(queryId: String) {
    Utils.TryWithRetry {
      logger.debug(s"Deleting query: $queryId")
      val key = computeRowKey(queryId)
      val mutator = HFactory.createMutator(keyspace, is)

      logger.debug(s"Deleting query state for: $queryId")
      val columnState = new Composite()
      columnState.setComponent(LEVEL_UUID, queryId, ss)
      columnState.setComponent(LEVEL_TYPE, TYPE_QUERY_STATE, is)
      mutator.addDeletion(key, CF_SPARK_LOGS, columnState, cs)

      logger.debug(s"Deleting query details for: $queryId")
      val columnScriptDetails = new Composite()
      columnScriptDetails.setComponent(LEVEL_TYPE, TYPE_SCRIPT_DETAILS, is)
      columnScriptDetails.setComponent(LEVEL_UUID, queryId, ss)
      mutator.addDeletion(key, CF_SPARK_LOGS, columnScriptDetails, cs)

      logger.debug(s"Deleting meta info for: $queryId")
      val columnMetaInfo = new Composite()
      columnMetaInfo.setComponent(LEVEL_TYPE, TYPE_META, is)
      columnMetaInfo.setComponent(LEVEL_UUID, queryId, ss)
      mutator.addDeletion(key, CF_SPARK_LOGS, columnMetaInfo, cs)

      logger.debug(s"Deleting query logs for: $queryId")
      var logs = getLogs(queryId, 0, 100)     
      while (logs.logs.isEmpty == false) {
        logs.logs.foreach(log => {
          println(log.timestamp)
          var columnLogs = new Composite()
          columnLogs.setComponent(LEVEL_TYPE, TYPE_LOG, is)
          columnLogs.setComponent(LEVEL_UUID, queryId, ss)
	      columnLogs.setComponent(LEVEL_TIME_STAMP, log.timestamp, ls)
          mutator.addDeletion(key, CF_SPARK_LOGS, columnLogs, cs)
        })
         mutator.execute()
         logs = getLogs(queryId, logs.logs(logs.logs.length -1).timestamp + 1, 100)
      }

      mutator.execute()

    }
  }
}