package apiactors

import akka.actor.Actor
import apiactors.ActorOperations._
import implementation.{AvroConverter, HiveContextWrapper}
import messages.GetDatasourceSchemaMessage
import org.apache.spark.scheduler.HiveUtils
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.parquet.ParquetUtils._
import server.Configuration

import scala.util.Try

/**
 * Created by lucianm on 06.02.2015.
 */
class GetDatasourceSchemaActor(hiveContext: HiveContextWrapper) extends Actor {

  object SourceType extends Enumeration {
    type SourceType = Value
    val PARQUET = Value("parquet")
    val HIVE = Value("hive")

    def asSourceType(`type`: String): SourceType = {
      if (`type`.equalsIgnoreCase(PARQUET.toString)) PARQUET
      else HIVE
    }
  }

  object StorageType extends Enumeration {
    type StorageType = Value
    val HDFS = Value("hdfs")
    val TACHYON = Value("tachyon")

    def asStorageType(`type`: String): StorageType = {
      if (`type`.equalsIgnoreCase(HDFS.toString)) HDFS
      else TACHYON
    }
  }


  def receive = {
    case request: GetDatasourceSchemaMessage =>

      val hostname: String = Configuration.rddDestinationIp.get
      val path: String = s"${request.path}"
      val sourceType: String = s"${request.sourceType}"
      val storageType: String = s"${request.storageType}"
      var message: String = s"Getting the datasource schema for path $path, sourceType $sourceType, storageType $storageType"
      Configuration.log4j.info(message)

      val response = Try {
        var result: StructType = null
        SourceType.asSourceType(sourceType) match {
          case SourceType.HIVE =>
            result = hiveContext.table(path).schema
          case SourceType.PARQUET =>
            StorageType.asStorageType(storageType) match {
              case StorageType.HDFS =>
                val hdfsURL = HiveUtils.getHdfsPath(hostname)
                result = hiveContext.readXPatternsParquet(hdfsURL, path).schema
              case StorageType.TACHYON =>
                val tachyonURL = HiveUtils.getTachyonPath(hostname)
                result = hiveContext.readXPatternsParquet(tachyonURL, path).schema
              case _ => Configuration.log4j.error("Unsupported type!")
            }
          case _ => Configuration.log4j.error("Unsupported type!")
        }

        val avroSchema = AvroConverter.getAvroSchema(result).toString(true)
        Configuration.log4j.info(avroSchema)
        message = avroSchema
      }
      returnResult(response, message, "GET datasource schema failed with the following message: ", sender)

    case request: Any => Configuration.log4j.error(request.toString)
  }

}
