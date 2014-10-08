package apiactors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import com.google.common.base.Preconditions
import com.xpatterns.jaws.data.DTO.Column
import com.xpatterns.jaws.data.DTO.Result
import com.xpatterns.jaws.data.utils.Utils
import server.Configuration
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.actorRef2Scala
import messages.GetResultsMessage
import net.liftweb.json._
import net.liftweb.json.DefaultFormats
import traits.DAL
import org.apache.spark.scheduler.HiveUtils

/**
 * Created by emaorhian
 */
class GetResultsApiActor(hdfsConf: org.apache.hadoop.conf.Configuration, hiveContext: HiveContext, dals: DAL) extends Actor {
 implicit val formats = DefaultFormats
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
        var result = dals.resultsDal.getResults(message.queryID)
        var endIndex = offset + limit
        if (endIndex > result.results.length) {
          endIndex = result.results.length
        }
        val res = result.results.slice(offset, endIndex)
        sender ! new Result(result.schema, res)

      } else {

        
        val schemaString = Utils.readFile(hdfsConf, Configuration.schemaFolder.getOrElse("jawsSchemaFolder") + "/" + message.queryID)
        val json = parse(schemaString)
        val schema = json.extract[Array[Column]]

        val resultsRDD: RDD[Tuple2[Object, Array[Object]]] = hiveContext.sparkContext.objectFile(HiveUtils.getHDFSRddPath(message.queryID, Configuration.jawsNamenode.get))
        val filteredResults = resultsRDD.filter(tuple => tuple._1.asInstanceOf[Long] >= offset && tuple._1.asInstanceOf[Long] < offset + limit).collect()

        sender ! Result.fromTuples(schema, filteredResults)
      }
    }
  }
}