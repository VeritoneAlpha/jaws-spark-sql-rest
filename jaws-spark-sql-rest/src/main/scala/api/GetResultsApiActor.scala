package api

import akka.actor.Actor
import akka.actor.actorRef2Scala
import messages.GetQueriesMessage
import com.google.common.base.Preconditions
import actors.LogsActor
import akka.actor.ActorLogging
import traits.DAL
import model.Queries
import model.Query
import messages.GetLogsMessage
import org.joda.time.DateTime
import java.util.Collection
import model.Logs
import model.Log
import messages.GetResultsMessage
import com.xpatterns.jaws.data.DTO.ScriptMetaDTO
import com.xpatterns.jaws.data.DTO.ResultDTO
import model.Result
import com.xpatterns.jaws.data.utils.Utils
import traits.CustomSharkContext
import actors.Configuration
import org.apache.spark.scheduler.SharkUtils
import org.apache.spark.rdd.RDD
/**
 * Created by emaorhian
 */
class GetResultsApiActor(hdfsConf: org.apache.hadoop.conf.Configuration, customSharkContext: CustomSharkContext, dals: DAL) extends Actor {

  override def receive = {

    case message: GetResultsMessage => {
      Configuration.log4j.info("[GetResultsMessage]: retrieving results for: " + message.queryID)
      Preconditions.checkArgument(message.queryID != null && !message.queryID.isEmpty(), Configuration.UUID_EXCEPTION_MESSAGE)
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

      val metaInfo = dals.loggingDal.getMetaInfo(message.queryID)
      if (metaInfo.resultsInCassandra == true) {
        val result = dals.resultsDal.getResults(message.queryID)
        var endIndex = offset + limit
        if (endIndex > result.results.size()) {
          endIndex = result.results.size()
        }
        result.results = result.results.subList(offset, endIndex)
        sender ! Result.fromResultDTO(result)

      } else {

        val schema = SharkUtils.getSchema(Utils.readFile(hdfsConf, Configuration.schemaFolder.getOrElse("jawsSchemaFolder") + "/" + message.queryID))

        val resultsRDD: RDD[Tuple2[Object, Array[Object]]] = customSharkContext.sharkContext.objectFile(SharkUtils.getHDFSRddPath(message.queryID, Configuration.jawsNamenode.get))
        val filteredResults = resultsRDD.filter(tuple => tuple._1.asInstanceOf[Long] >= offset && tuple._1.asInstanceOf[Long] < offset + limit).collect()

        sender ! Result.fromTuples(schema, filteredResults)
      }
    }
  }
}