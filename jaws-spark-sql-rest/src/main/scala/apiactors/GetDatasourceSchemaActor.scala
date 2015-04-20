package apiactors

import akka.actor.Actor
import apiactors.ActorOperations._
import implementation.SchemaSettingsFactory.{ Hdfs, Hive, Parquet, Tachyon }
import implementation.{ AvroConverter, HiveContextWrapper }
import messages.GetDatasourceSchemaMessage
import org.apache.spark.scheduler.HiveUtils
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.parquet.ParquetUtils._
import server.Configuration
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage

/**
 * Created by lucianm on 06.02.2015.
 */
class GetDatasourceSchemaActor(hiveContext: HiveContextWrapper) extends Actor {

  def receive = {
    case request: GetDatasourceSchemaMessage => {

      val hostname: String = Configuration.rddDestinationIp.get
      val path: String = s"${request.path}"
      Configuration.log4j.info(s"Getting the data source schema for path $path, sourceType ${request.sourceType}, storageType ${request.storageType}")
      val currentSender = sender

      val getDatasourceSchemaFuture = future {
        var result: StructType = null
        request.sourceType match {
          case Hive() =>
            result = hiveContext.table(path).schema
          case Parquet() =>
            request.storageType match {
              case Hdfs() =>
                val hdfsURL = HiveUtils.getHdfsPath(hostname)
                result = hiveContext.readXPatternsParquet(hdfsURL, path).schema
              case Tachyon() =>
                val tachyonURL = HiveUtils.getTachyonPath(hostname)
                result = hiveContext.readXPatternsParquet(tachyonURL, path).schema
            }
        }

        val avroSchema = AvroConverter.getAvroSchema(result).toString(true)
        Configuration.log4j.debug(avroSchema)
        avroSchema
      }

      getDatasourceSchemaFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET data source schema failed with the following message: ${e.getMessage}")
      }
    }
    case request: Any => Configuration.log4j.error(request.toString)
  }

}
