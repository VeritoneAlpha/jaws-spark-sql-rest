package apiactors

import apiactors.ActorOperations._
import scala.concurrent._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import com.google.common.base.Preconditions
import com.xpatterns.jaws.data.DTO.Column
import com.xpatterns.jaws.data.utils.Utils
import server.Configuration
import akka.actor.Actor
import akka.actor.actorRef2Scala
import messages.GetResultsMessage
import net.liftweb.json._
import net.liftweb.json.DefaultFormats
import com.xpatterns.jaws.data.contracts.DAL
import org.apache.spark.sql.hive.HiveUtils
import ExecutionContext.Implicits.global
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import messages.ErrorMessage
import messages.ResultFormat._
import com.xpatterns.jaws.data.DTO.AvroResult
import com.xpatterns.jaws.data.DTO.CustomResult
import com.xpatterns.jaws.data.utils.ResultsConverter
import org.apache.spark.sql.Row
import com.xpatterns.jaws.data.DTO.AvroBinaryResult

/**
 * Created by emaorhian
 */
class GetResultsApiActor(hdfsConf: org.apache.hadoop.conf.Configuration, hiveContext: HiveContext, dals: DAL) extends Actor {
  implicit val formats = DefaultFormats
  override def receive = {

    case message: GetResultsMessage =>
      {
        Configuration.log4j.info(s"[GetResultsMessage]: retrieving results for: ${message.queryID} in the ${message.format}")
        val currentSender = sender
        val getResultsFuture = future {

          val (offset, limit) = getOffsetAndLimit(message)
          val metaInfo = dals.loggingDal.getMetaInfo(message.queryID)

          metaInfo.resultsDestination match {
            // cassandra
            case 0 => {
              var endIndex = offset + limit
              message.format match {
                case AVRO_BINARY_FORMAT => new AvroBinaryResult(getDBAvroResults(message.queryID, offset, endIndex))
                case AVRO_JSON_FORMAT   => getDBAvroResults(message.queryID, offset, endIndex).result
                case _                  => getCustomResults(message.queryID, offset, endIndex)
              }

            }
            //hdfs
            case 1 => {
              val destinationPath = HiveUtils.getHdfsPath(Configuration.rddDestinationIp.get)
              getFormattedResult(message.format, getResults(offset, limit, destinationPath))

            }
            //tachyon
            case 2 => {
              val destinationPath = HiveUtils.getTachyonPath(Configuration.rddDestinationIp.get)
              getFormattedResult(message.format, getResults(offset, limit, destinationPath))

            }
            case _ => {
              Configuration.log4j.info("[GetResultsMessage]: Unidentified results path : " + metaInfo.resultsDestination)
              null
            }
          }
        }

        getResultsFuture onComplete {
          case Success(results) => currentSender ! results
          case Failure(e)       => currentSender ! ErrorMessage(s"GET results failed with the following message: ${e.getMessage}")
        }

      }

      def getResults(offset: Int, limit: Int, destinationPath: String): ResultsConverter = {
        val schemaBytes = Utils.readBytes(hdfsConf, Configuration.schemaFolder.getOrElse("jawsSchemaFolder") + "/" + message.queryID)
        val schema = HiveUtils.deserializaSchema(schemaBytes)

        val resultsRDD: RDD[Tuple2[Object, Array[Object]]] = hiveContext.sparkContext.objectFile(HiveUtils.getRddDestinationPath(message.queryID, destinationPath))

        val filteredResults = resultsRDD.filter(tuple => tuple._1.asInstanceOf[Long] >= offset && tuple._1.asInstanceOf[Long] < offset + limit).collect()

        val resultRows = filteredResults map { case (index, row) => Row.fromSeq(row) }

        new ResultsConverter(schema, resultRows)

      }
  }

  def getOffsetAndLimit(message: GetResultsMessage): (Int, Int) = {
    var offset = message.offset
    var limit = message.limit

    Option(offset) match {
      case None => {
        Configuration.log4j.info("[GetResultsMessage]: offset null... setting it on 0")
        offset = 0
      }
      case _ => {
        Configuration.log4j.info("[GetResultsMessage]: offset = " + offset)
      }
    }

    Option(limit) match {
      case None => {
        Configuration.log4j.info("[GetResultsMessage]: limit null... setting it on 100")
        limit = 100
      }
      case _ => {
        Configuration.log4j.info("[GetResultsMessage]: limit = " + limit)
      }
    }
    (offset, limit)
  }

  private def getDBAvroResults(queryID: String, offset: Int, limit: Int) = {
    val result = dals.resultsDal.getAvroResults(queryID)
    val lastResultIndex = if (limit > result.result.length) result.result.length else limit
    new AvroResult(result.schema, result.result.slice(offset, lastResultIndex))
  }

  private def getCustomResults(queryID: String, offset: Int, limit: Int) = {
    val result = dals.resultsDal.getCustomResults(queryID)
    val lastResultIndex = if (limit > result.result.length) result.result.length else limit
    new CustomResult(result.schema, result.result.slice(offset, lastResultIndex))
  }

  private def getFormattedResult(format: String, resultsConverter: ResultsConverter) = {
    format match {
      case AVRO_BINARY_FORMAT => resultsConverter.toAvroBinaryResults()
      case AVRO_JSON_FORMAT   => resultsConverter.toAvroResults().result
      case _                  => resultsConverter.toCustomResults()
    }
  }
}