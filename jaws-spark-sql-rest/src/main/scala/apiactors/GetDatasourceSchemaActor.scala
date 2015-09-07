package apiactors

import akka.actor.Actor
import implementation.SchemaSettingsFactory.{ Hdfs, Hive, Parquet, Tachyon }
import implementation.HiveContextWrapper
import messages.GetDatasourceSchemaMessage
import org.apache.spark.scheduler.HiveUtils
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.parquet.SparkParquetUtility._
import server.Configuration
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage
import com.xpatterns.jaws.data.utils.{Utils, AvroConverter}
import org.apache.spark.sql.types.StructType

/**
 * Handles the operations used for getting the schema
 */
class GetDatasourceSchemaActor(hiveContext: HiveContextWrapper) extends Actor {

  def receive = {
    case request: GetDatasourceSchemaMessage =>
      val hostname: String = Configuration.rddDestinationIp.get
      val path: String = s"${request.path}"
      Configuration.log4j.info(s"Getting the data source schema for path $path, sourceType ${request.sourceType}, storageType ${request.storageType}")
      val currentSender = sender()

      val getDatasourceSchemaFuture = future {
        var result: StructType = null
        request.sourceType match {
          case Hive() =>

            try {
              val table = hiveContext.table(path)
              result = table.schema
            } catch {
              // When the table doesn't exists, throw a new exception with a better message.
              case _:NoSuchTableException => throw new Exception("Table does not exist")
            }
          case Parquet() =>
            request.storageType match {
              case Hdfs() =>
                val hdfsURL = HiveUtils.getHdfsPath(hostname)

                // Make sure that file exists, otherwise an NPE will be thrown.
                val newConf = new org.apache.hadoop.conf.Configuration(request.hdfsConf)
                newConf.set("fs.defaultFS", hdfsURL)
                if (!Utils.checkFileExistence(hdfsURL + path, newConf)) {
                  throw new Exception("File path does not exist")
                }

                result = hiveContext.readXPatternsParquet(hdfsURL, path).schema
              case Tachyon() =>
                val tachyonURL = HiveUtils.getTachyonPath(hostname)

                // Make sure that file exists, otherwise an NPE will be thrown.
                val newConf = new org.apache.hadoop.conf.Configuration(request.hdfsConf)
                newConf.set("fs.defaultFS", tachyonURL)
                if (!Utils.checkFileExistence(tachyonURL + path, newConf)) {
                  throw new Exception("File path does not exist")
                }

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

    case request: Any => Configuration.log4j.error(request.toString)
  }

}
