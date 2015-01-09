package org.apache.spark.sql.parquet

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Row
import parquet.hadoop.ParquetWriter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.types.StructType
import scala.reflect.runtime.universe.TypeTag
import parquet.column.ParquetProperties.WriterVersion
import parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.catalyst.expressions.Attribute
import parquet.hadoop.ParquetReader
import org.apache.spark.sql.SchemaRDDLike
import org.apache.spark.rdd.RDD
import java.io.IOException
import scala.reflect.ClassTag
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.io.InputStreamReader
import java.io.BufferedReader
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import parquet.hadoop.ParquetFileWriter
import parquet.hadoop.metadata.ParquetMetadata
import org.apache.hadoop.fs.FSDataOutputStream
import parquet.format.converter.ParquetMetadataConverter
import parquet.hadoop.Footer
import parquet.hadoop.metadata.BlockMetaData
import parquet.io.ParquetEncodingException
import tachyon.client.WriteType
import java.net.URL

object ParquetUtils {

  implicit class xPatternsSQL(sqlC: SQLContext) {
    def readXPatternsParquet(nameNodeUrl: String, folderPath: String) = {
      var nameNode = sanitizePath(nameNodeUrl)
      var path = sanitizePath(folderPath)
      val finalReadPath = nameNode + "/" + path

      val metadata = readMetadata(sqlC.sparkContext, nameNode, path)
      val objectAttributes = ParquetTypesConverter.convertFromString(metadata)

      val filePathRDD = getAllParquetParts(sqlC.sparkContext, nameNode, path)

      val rowRDD = filePathRDD.flatMap(fileName => {
        var objectList = scala.collection.mutable.MutableList[Row]()
        val parquetReader = getParquetReader(finalReadPath + "/" + fileName, objectAttributes)
        Stream.continually(parquetReader.read()).takeWhile(_ ne null) foreach { row => objectList += row.copy() }
        parquetReader.close()
        objectList
      })

      val schemaRDD = sqlC.applySchema(rowRDD, getSchemaFromAttributes(objectAttributes))
      schemaRDD
    }
  }

  implicit class xPatternsRDD[T <: Product: TypeTag: ClassTag](rdd: RDD[T]) {
    def saveAsXPatternsParquet(nameNodeUrl: String, folderPath: String) = {
      var nameNode = sanitizePath(nameNodeUrl)
      var path = sanitizePath(folderPath)
      val finalWritePath = nameNode + "/" + path

      val attributes = getAttributesList[T]

      writeMetadata(rdd.context, nameNode, attributes, finalWritePath)
      val errorRDD = rdd.mapPartitionsWithIndex((index, iterator) => {

        val errorList = scala.collection.mutable.MutableList[T]()
        val parquetWriter = getParquetWriter(finalWritePath + "/part-" + index + ".parquet", attributes)

        while (iterator.hasNext) {
          val objectToWrite = iterator.next()
          try {
            parquetWriter.write(transformObjectToRow(objectToWrite))
          } catch {
            case e: IOException => errorList += objectToWrite
          }
        }

        parquetWriter.close()
        errorList.toList.iterator
      })

      (errorRDD.count(), errorRDD)
    }
  }

  implicit class xPatternsSchemaRDD(schemaRDD: SchemaRDD) {
    def saveAsXPatternsParquet(nameNodeUrl: String, folderPath: String) = {
      var nameNode = sanitizePath(nameNodeUrl)
      var path = sanitizePath(folderPath)
      val finalWritePath = nameNode + "/" + path

      val attributes = schemaRDD.schema.toAttributes
      println(attributes)

      writeMetadata(schemaRDD.sqlContext.sparkContext, nameNode, attributes, finalWritePath)
      val errorRDD = schemaRDD.mapPartitionsWithIndex((index, iterator) => {
        val errorList = scala.collection.mutable.MutableList[Row]()
        val parquetWriter = getParquetWriter(finalWritePath + "/part-" + index + ".parquet", attributes)

        while (iterator.hasNext) {
          val objectToWrite = iterator.next()
          try {
            parquetWriter.write(objectToWrite)
          } catch {
            case e: IOException => errorList += objectToWrite
          }
        }

        parquetWriter.close()
        errorList.toList.iterator
      })

      (errorRDD.count(), errorRDD)
    }
  }

  def transformObjectToRow[A <: Product](data: A): Row = {
    val mutableRow = new GenericMutableRow(data.productArity)
    var i = 0
    while (i < mutableRow.length) {
      mutableRow(i) = ScalaReflection.convertToCatalyst(data.productElement(i))
      i += 1
    }
    mutableRow
  }

  def getAttributesList[T <: Product: TypeTag]: Seq[Attribute] = {
    val att = ScalaReflection.attributesFor[T]
    println("Atributes from schema: " + att.map { x => x + ";" })
    att
  }

  def getSchemaFromAttributes(attributes: Seq[Attribute]): StructType = {
    StructType.fromAttributes(attributes)
  }
    
  def getParquetWriter(filePath: String, attributes: Seq[Attribute]): ParquetWriter[Row] = {

    val conf = new Configuration()
    val writeSupport = new RowWriteSupport
    RowWriteSupport.setSchema(attributes, conf)

    new ParquetWriter[Row](new Path(filePath),
      writeSupport,
      ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
      ParquetWriter.DEFAULT_BLOCK_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
      ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
      ParquetWriter.DEFAULT_WRITER_VERSION,
      conf)
  }

  def getParquetReader(filePath: String, attributes: Seq[Attribute]): ParquetReader[Row] = {
    val conf = new Configuration()
    val readSupport = new RowReadSupport
    val encoded = ParquetTypesConverter.convertToString(attributes)
    conf.set(RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA, encoded)
    new ParquetReader(conf, new Path(filePath), readSupport, null)
  }

  private def writeMetadata(sc: SparkContext, nameNode: String, attributes: Seq[Attribute], folderPath: String) = {
    val metdataRdd = sc.parallelize(List(folderPath))
    metdataRdd.foreach { path =>
      val conf = new Configuration()
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, nameNode)
      ParquetTypesConverter.writeMetaData(attributes, new Path(path), conf)
    }
  }

  private def readMetadata(sc: SparkContext, nameNode: String, folederPath: String) = {
    val metdataRdd = sc.parallelize(List(folederPath))
    metdataRdd.map { path =>
      val conf = new Configuration()
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, nameNode)
      ParquetTypesConverter.readMetaData(new Path(nameNode + "/" + path), Option(conf)).getFileMetaData.getKeyValueMetaData.get(RowReadSupport.SPARK_METADATA_KEY)
    }.first
  }

  private def getAllParquetParts(sc: SparkContext, nameNodeUrl: String, folderPath: String): RDD[String] = {
    val parquetFolderPath = nameNodeUrl + "/" + folderPath + "/"
    val parquetFolderRddPath: RDD[String] = sc.parallelize(List(parquetFolderPath))

    val rdd = parquetFolderRddPath.flatMap[String](path => {
      val conf: Configuration = new Configuration()
      val fs = FileSystem.get(URI.create(nameNodeUrl), conf)
      val filesList = fs.listFiles(new Path(path), false)
      var pathListResult = List[String]()

      while (filesList.hasNext()) {
        val fileName = filesList.next().getPath.getName
        if (fileName.endsWith(".parquet")) {
          pathListResult = pathListResult ::: List(fileName)
        }
      }

      pathListResult
    })
    rdd
  }

  private def sanitizePath(path: String) = {
    var pathIntermediar = if (path.endsWith("/")) path.substring(0, path.size - 1) else path
    if (pathIntermediar.startsWith("/")) 
       pathIntermediar.substring(1, pathIntermediar.size) 
    else pathIntermediar
  }
}
