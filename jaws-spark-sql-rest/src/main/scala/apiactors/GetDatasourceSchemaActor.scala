package apiactors

import akka.actor.Actor
import implementation.SchemaSettingsFactory.{ Hdfs, Hive, Parquet, Tachyon }
import implementation.HiveContextWrapper
import messages.GetDatasourceSchemaMessage
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.datasources.parquet.SparkParquetUtility._
import server.Configuration
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import messages.ErrorMessage
import com.xpatterns.jaws.data.utils.{Utils, AvroConverter}
import org.apache.spark.sql.types.StructType
import com.xpatterns.jaws.data.utils.Utils._
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

                // Make sure that file exists
                checkFileExistence(request.hdfsConf, hdfsURL, path)

                result = hiveContext.readXPatternsParquet(hdfsURL, path).schema
              case Tachyon() =>
                val tachyonURL = HiveUtils.getTachyonPath(hostname)

                // Make sure that file exists
                checkFileExistence(request.hdfsConf, tachyonURL, path)

                result = hiveContext.readXPatternsParquet(tachyonURL, path).schema
            }
        }

        Configuration.log4j.info("Reading the avro schema from result df")
      
        val avroSchema = AvroConverter.getAvroSchema(result).toString(true)
        Configuration.log4j.debug(avroSchema)
        avroSchema
      }

      getDatasourceSchemaFuture onComplete {
        case Success(result) => currentSender ! result
        case Failure(e) => currentSender ! ErrorMessage(s"GET data source schema failed with the following message: ${getCompleteStackTrace(e)}")
      }

    case request: Any => Configuration.log4j.error(request.toString)
  }

  /**
   * Checks the file existence on the sent file system. If the file is not found an exception is thrown
   * @param hdfsConfiguration the hdfs configuration
   * @param defaultFSUrl the file system default path. It is different for hdfs and for tachyon.
   * @param filePath the path for the file for which the existence is checked
   */
  private def checkFileExistence(hdfsConfiguration: org.apache.hadoop.conf.Configuration, defaultFSUrl:String, filePath:String) = {
    val newConf = new org.apache.hadoop.conf.Configuration(hdfsConfiguration)
    newConf.set("fs.defaultFS", defaultFSUrl)
    if (!Utils.checkFileExistence(defaultFSUrl + filePath, newConf)) {
      throw new Exception("File path does not exist")
    }
  }
}
