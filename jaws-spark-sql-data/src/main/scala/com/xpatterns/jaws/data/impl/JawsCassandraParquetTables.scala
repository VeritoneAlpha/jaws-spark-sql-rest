package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsParquetTables
import com.xpatterns.jaws.data.DTO.ParquetTable
import com.xpatterns.jaws.data.utils.Utils
import org.apache.log4j.Logger
import me.prettyprint.hector.api.beans.Composite
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.Keyspace
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.serializers.CompositeSerializer
import me.prettyprint.cassandra.serializers.LongSerializer
import spray.json._
import spray.json.DefaultJsonProtocol._

class JawsCassandraParquetTables(keyspace: Keyspace) extends TJawsParquetTables {

  val CF_PARQUET_TABLES = "parquet_tables"
  val ROW_ID = "tables"

  val is = IntegerSerializer.get.asInstanceOf[Serializer[Int]]
  val ss = StringSerializer.get.asInstanceOf[Serializer[String]]
  val cs = CompositeSerializer.get.asInstanceOf[Serializer[Composite]]
  val ls = LongSerializer.get.asInstanceOf[Serializer[Long]]

  val logger = Logger.getLogger("JawsCassandraParquetTables")

  override def addParquetTable(pTable: ParquetTable) {
    Utils.TryWithRetry {
      logger.debug(s"Adding the parquet table ${pTable.name} for the filepath ${pTable.filePath}")
      val mutator = HFactory.createMutator(keyspace, ss)

      val valueTouple = (pTable.namenode, pTable.filePath).toJson.prettyPrint
      mutator.addInsertion(ROW_ID, CF_PARQUET_TABLES, HFactory.createColumn(pTable.name, valueTouple, ss, ss))
      mutator.execute()
    }
  }

  override def deleteParquetTable(name: String) {
    Utils.TryWithRetry {
      logger.debug(s"Deleting parquet table $name")

      val mutator = HFactory.createMutator(keyspace, ss)

      mutator.addDeletion(ROW_ID, CF_PARQUET_TABLES, name, ss)
      mutator.execute
    }
  }
  override def listParquetTables(): Array[ParquetTable] = {
    Utils.TryWithRetry {
      var result = Array[ParquetTable]()
      logger.debug("listing all parquet tables")
      val sliceQuery = HFactory.createSliceQuery(keyspace, ss, ss, ss).setColumnFamily(CF_PARQUET_TABLES).setKey(ROW_ID).setRange(null, null, false, Int.MaxValue)
      val queryResult = sliceQuery.execute
      Option(queryResult) match {
        case None => result
        case _ => {
          val columnSlice = queryResult.get
          Option(columnSlice) match {
            case None => result
            case _ => {
              val columns = columnSlice.getColumns
              Option(columns) match {
                case None => result
                case _ => {
                  columns.size match {
                    case 0 => result
                    case size: Int => {
                      for (index <- 0 until size) {
                        val column = columns.get(index)
                        val (namenode, filepath) = column.getValue.parseJson.convertTo[(String, String)]
                        result = result :+ new ParquetTable(column.getName, filepath, namenode)
                      }
                      result
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  override def tableExists(name: String): Boolean = {
    Utils.TryWithRetry {
      logger.debug(s"Reading the parquet table $name")
      val columnQuery = HFactory.createColumnQuery(keyspace, ss, ss, ss)
      columnQuery.setColumnFamily(CF_PARQUET_TABLES).setKey(ROW_ID).setName(name)

      val queryResult = columnQuery.execute
      Option(queryResult) match {
        case None => false
        case _ => {
          val column = queryResult.get
          Option(column) match {
            case None => false
            case _ => true
          }
        }
      }
    }
  }

  override def readParquetTable(name: String): ParquetTable = {
    Utils.TryWithRetry {
      logger.debug(s"Reading the parquet table $name")
      val columnQuery = HFactory.createColumnQuery(keyspace, ss, ss, ss)
      columnQuery.setColumnFamily(CF_PARQUET_TABLES).setKey(ROW_ID).setName(name)

      val queryResult = columnQuery.execute
      Option(queryResult) match {
        case None => new ParquetTable
        case _ => {
          val column = queryResult.get
          Option(column) match {
            case None => new ParquetTable
            case _ => 
              {
                 val (namenode, filepath) = column.getValue.parseJson.convertTo[(String, String)]
                 new ParquetTable(column.getName, filepath, namenode)
              }
             
          }
        }
      }
    }
  }
}