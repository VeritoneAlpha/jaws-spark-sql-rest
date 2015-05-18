package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.utils.Utils
import me.prettyprint.hector.api.Keyspace
import com.xpatterns.jaws.data.contracts.TJawsResults
import org.apache.log4j.Logger
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.beans.ColumnSlice
import me.prettyprint.hector.api.query.QueryResult
import net.liftweb.json.DefaultFormats
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import net.liftweb.json._
import spray.json._
import me.prettyprint.hector.api.beans.Composite
import me.prettyprint.cassandra.serializers.CompositeSerializer
import me.prettyprint.cassandra.serializers.BytesArraySerializer
import com.xpatterns.jaws.data.utils.ResultsConverter
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality
import scala.collection.JavaConversions._
import com.xpatterns.jaws.data.DTO.AvroResult
import com.xpatterns.jaws.data.DTO.CustomResult
import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema

class JawsCassandraResults(keyspace: Keyspace) extends TJawsResults {

  val CF_SPARK_RESULTS = "results"
  val CF_SPARK_RESULTS_NUMBER_OF_ROWS = 100

  val LEVEL_UUID = 0
  val LEVEL_FORMAT = 1
  val LEVEL_TYPE = 2

  val FORMAT_AVRO = "avro"
  val FORMAT_CUSTOM = "custom"

  val TYPE_SCHEMA = 1
  val TYPE_RESULT = 2

  val logger = Logger.getLogger("JawsCassandraResults")

  val is = IntegerSerializer.get.asInstanceOf[Serializer[Int]]
  val ss = StringSerializer.get.asInstanceOf[Serializer[String]]
  val cs = CompositeSerializer.get.asInstanceOf[Serializer[Composite]]
  val bs = BytesArraySerializer.get.asInstanceOf[Serializer[Array[Byte]]]

  def setAvroResults(uuid: String, avroResults: AvroResult) {
    Utils.TryWithRetry {

      logger.debug("Writing avro results to query " + uuid)

      val key = computeRowKey(uuid)

      val columnResult = new Composite()
      columnResult.setComponent(LEVEL_UUID, uuid, ss)
      columnResult.setComponent(LEVEL_FORMAT, FORMAT_AVRO, ss)
      columnResult.setComponent(LEVEL_TYPE, TYPE_RESULT, is)

      val columnSchema = new Composite()
      columnSchema.setComponent(LEVEL_UUID, uuid, ss)
      columnSchema.setComponent(LEVEL_FORMAT, FORMAT_AVRO, ss)
      columnSchema.setComponent(LEVEL_TYPE, TYPE_SCHEMA, is)

      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addInsertion(key, CF_SPARK_RESULTS, HFactory.createColumn(columnResult, avroResults.serializeResult(), cs, bs))
      mutator.addInsertion(key, CF_SPARK_RESULTS, HFactory.createColumn(columnSchema, avroResults.schema.toString(), cs, ss))

      mutator.execute()
    }
  }

  def getAvroResults(uuid: String): AvroResult = {
    Utils.TryWithRetry {

      logger.debug("Reading results for query: " + uuid)

      val key = computeRowKey(uuid)
      val schema = new Composite()

      schema.addComponent(LEVEL_UUID, uuid, ComponentEquality.EQUAL)
      schema.addComponent(LEVEL_FORMAT, FORMAT_AVRO, ComponentEquality.EQUAL)
      schema.addComponent(LEVEL_TYPE, TYPE_SCHEMA, ComponentEquality.EQUAL)

      val results = new Composite()
      results.addComponent(LEVEL_UUID, uuid, ComponentEquality.EQUAL)
      results.addComponent(LEVEL_FORMAT, FORMAT_AVRO, ComponentEquality.EQUAL)
      results.addComponent(LEVEL_TYPE, TYPE_RESULT, ComponentEquality.EQUAL)

      val columnQuery = HFactory.createSliceQuery(keyspace, is, cs, bs)
      columnQuery.setColumnFamily(CF_SPARK_RESULTS).setKey(key).setColumnNames(schema, results)

      val queryResult = columnQuery.execute()
      Option(queryResult) match {
        case None => new AvroResult()
        case _ => {
          val columnSlice = queryResult.get()
          Option(columnSlice) match {
            case None => new AvroResult()
            case _ => {
              Option(columnSlice.getColumns()) match {
                case None => new AvroResult()
                case _ => {

                  columnSlice.getColumns().size() match {
                    case 2 => {
                      var sch: Schema = null
                      var resultByteArray: Array[Byte] = null
                      columnSlice.getColumns.foreach(col => {

                        col.getName.getComponent(LEVEL_TYPE).getValue(is) match {
                          case TYPE_SCHEMA => {
                            val schemaParser = new Schema.Parser()
                            sch = schemaParser.parse(new String(col.getValue()))
                          }
                          case _ => {
                            resultByteArray = col.getValue()
                          }
                        }
                      })

                      val res = AvroResult.deserializeResult(resultByteArray, sch)
                      new AvroResult(sch, res)
                    }
                    case _ => new AvroResult()
                  }

                }
              }
            }
          }

        }
      }
    }
  }

  def getCustomResults(uuid: String): CustomResult = {
    Utils.TryWithRetry {

      logger.debug("Reading custom results for query: " + uuid)

      val key = computeRowKey(uuid)
      val column = new Composite()

      column.addComponent(LEVEL_UUID, uuid, ComponentEquality.EQUAL)
      column.addComponent(LEVEL_FORMAT, FORMAT_CUSTOM, ComponentEquality.EQUAL)

      val columnQuery = HFactory.createColumnQuery(keyspace, is, cs, ss)
      columnQuery.setColumnFamily(CF_SPARK_RESULTS).setKey(key).setName(column)

      val queryResult = columnQuery.execute()
      Option(queryResult) match {
        case None => new CustomResult()
        case _ => {
          val hColumn = queryResult.get()
          Option(hColumn) match {
            case None => new CustomResult()
            case _ => {
              implicit val formats = DefaultFormats
              val value = hColumn.getValue()
              val json = parse(value)
              json.extract[CustomResult]
            }
          }
        }
      }
    }
  }

  def setCustomResults(uuid: String, results: CustomResult) {
    Utils.TryWithRetry {
      logger.debug("Writing custom results to query " + uuid)

      val key = computeRowKey(uuid)

      val column = new Composite()
      column.setComponent(LEVEL_UUID, uuid, ss)
      column.setComponent(LEVEL_FORMAT, FORMAT_CUSTOM, ss)

      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addInsertion(key, CF_SPARK_RESULTS, HFactory.createColumn(column, results.toJson.toString(), cs, ss))

      mutator.execute()
    }
  }

  private def computeRowKey(uuid: String): Integer = {
    Math.abs(uuid.hashCode() % CF_SPARK_RESULTS_NUMBER_OF_ROWS)
  }

  def deleteResults(uuid: String) {
    Utils.TryWithRetry {

      logger.debug(s"Deleting results for query $uuid")

      val key = computeRowKey(uuid)
      val mutator = HFactory.createMutator(keyspace, is)

      val columnResult = new Composite()
      columnResult.setComponent(LEVEL_UUID, uuid, ss)
      columnResult.setComponent(LEVEL_FORMAT, FORMAT_AVRO, ss)
      columnResult.setComponent(LEVEL_TYPE, TYPE_RESULT, is)

      val columnSchema = new Composite()
      columnSchema.setComponent(LEVEL_UUID, uuid, ss)
      columnSchema.setComponent(LEVEL_FORMAT, FORMAT_AVRO, ss)
      columnSchema.setComponent(LEVEL_TYPE, TYPE_SCHEMA, is)

      val columnCustom = new Composite()
      columnCustom.setComponent(LEVEL_UUID, uuid, ss)
      columnCustom.setComponent(LEVEL_FORMAT, FORMAT_CUSTOM, ss)

      mutator.addDeletion(key, CF_SPARK_RESULTS, columnSchema, cs)
      mutator.addDeletion(key, CF_SPARK_RESULTS, columnResult, cs)
      mutator.addDeletion(key, CF_SPARK_RESULTS, columnCustom, cs)
      mutator.execute()
    }
  }
}
