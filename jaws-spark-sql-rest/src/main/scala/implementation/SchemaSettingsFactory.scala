package implementation

import server.Configuration

/**
 * Created by lucianm on 12.02.2015.
 */
object SchemaSettingsFactory {

  trait SourceType

  case class Parquet() extends SourceType

  case class Hive() extends SourceType

  trait StorageType

  case class Hdfs() extends StorageType

  case class Tachyon() extends StorageType

  val HIVE: String = "hive"

  val PARQUET: String = "parquet"

  def getSourceType(sourceType: String): SourceType = {
    if (sourceType.equalsIgnoreCase(HIVE)) new Hive
    else if (sourceType.equalsIgnoreCase(PARQUET)) new Parquet
    else throw new Exception(Configuration.UNSUPPORTED_SOURCE_TYPE)
  }

  val HDFS: String = "hdfs"

  val TACHYON: String = "tachyon"

  def getStorageType(storageType: String): StorageType = {
    if (storageType.equalsIgnoreCase(HDFS)) new Hdfs
    else if (storageType.equalsIgnoreCase(TACHYON)) new Tachyon
    else throw new Exception(Configuration.UNSUPPORTED_STORAGE_TYPE)
  }

}
