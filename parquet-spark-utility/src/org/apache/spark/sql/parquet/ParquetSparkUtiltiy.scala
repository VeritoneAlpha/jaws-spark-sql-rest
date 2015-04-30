package org.apache.spark.sql.parquet

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.SparkHadoopMapReduceUtil
import org.apache.hadoop.util.StringUtils
import org.apache.spark.SerializableWritable
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.SchemaRDDLike
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.StructType
import parquet.column.ParquetProperties.WriterVersion
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.ParquetWriter
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.metadata.ParquetMetadata
import parquet.hadoop.util.ContextUtil
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object ParquetSparkUtility extends SparkHadoopMapReduceUtil with Serializable {

  implicit val defaultParquetBlockSize = ParquetWriter.DEFAULT_BLOCK_SIZE
  implicit val defaultCodec = CompressionCodecName.UNCOMPRESSED

  implicit class xPatternsSQL(sqlC: SQLContext) {
    def readXPatternsParquet(nameNodeUrl: String, path: String) = {

      var nameNode = sanitizePath(nameNodeUrl)
      var finalPath = sanitizePath(path)
      val sc = sqlC.sparkContext

      val metadata = readMetadata(sc, nameNode, path)
      val inputSplitsList = getInputSplits(sc, nameNode, path, metadata)
      val inputSplitsRdd = sc.parallelize(inputSplitsList, inputSplitsList.size)
      val id = inputSplitsRdd.id

      val rdd = inputSplitsRdd.flatMap(tuplePath => {
        val (conf, job, inputformat) = initializeJob(metadata, nameNode)

        val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, 0, 0)
        val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)

        val reader = inputformat.createRecordReader(tuplePath.t, hadoopAttemptContext)
        reader.initialize(tuplePath.t, hadoopAttemptContext)

        val partitionList = ListBuffer[Row]()
        while (reader.nextKeyValue()) {
          partitionList.+=(reader.getCurrentValue().copy)
        }
        
        partitionList
      })

      val schemaRDD = sqlC.applySchema(rdd, getSchemaFromAttributes(ParquetTypesConverter.convertFromString(metadata)))
      schemaRDD
    }
  }

  implicit class xPatternsRDD[T <: Product: TypeTag: ClassTag](rdd: RDD[T]) {

    def saveAsXPatternsParquet(nameNodeUrl: String, folderPath: String, codec: CompressionCodecName = defaultCodec, parquetBlockSize: Int = defaultParquetBlockSize) = {
      var nameNode = sanitizePath(nameNodeUrl)
      var path = sanitizePath(folderPath)
      val finalWritePath = nameNode + "/" + path

      val attributes = getAttributesList[T]

      writeMetadata(rdd.context, nameNode, attributes, finalWritePath)
      val errorRDD = rdd.mapPartitionsWithIndex((index, iterator) => {

        val errorList = scala.collection.mutable.MutableList[T]()
        val parquetWriter = getParquetWriter(finalWritePath + "/part-" + index + ".parquet", attributes, parquetBlockSize = parquetBlockSize, codec = codec)

        while (iterator.hasNext) {
          val objectToWrite = iterator.next()
          try {
            parquetWriter.write(transformObjectToRow(objectToWrite))
          } catch {
            case e: Exception => errorList += objectToWrite
          }
        }

        parquetWriter.close()
        errorList.toList.iterator
      })

      (errorRDD.count(), errorRDD)
    }
  }

  implicit class xPatternsSchemaRDD(schemaRDD: SchemaRDD) {

    def saveAsXPatternsParquet(nameNodeUrl: String, folderPath: String, codec: CompressionCodecName = defaultCodec, parquetBlockSize: Int = defaultParquetBlockSize) = {
      var nameNode = sanitizePath(nameNodeUrl)
      var path = sanitizePath(folderPath)
      val finalWritePath = nameNode + "/" + path

      val attributes = schemaRDD.schema.toAttributes
      println(attributes)

      writeMetadata(schemaRDD.sqlContext.sparkContext, nameNode, attributes, finalWritePath)
      val errorRDD = schemaRDD.mapPartitionsWithIndex((index, iterator) => {
        val errorList = scala.collection.mutable.MutableList[Row]()
        val parquetWriter = getParquetWriter(finalWritePath + "/part-" + index + ".parquet", attributes, parquetBlockSize = parquetBlockSize, codec = codec)

        while (iterator.hasNext) {
          val objectToWrite = iterator.next()
          try {
            parquetWriter.write(objectToWrite)
          } catch {
            case e: Exception => errorList += objectToWrite
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

  def getParquetWriter(filePath: String, attributes: Seq[Attribute], codec: CompressionCodecName = defaultCodec, parquetBlockSize: Int = defaultParquetBlockSize): ParquetWriter[Row] = {

    val conf = new Configuration()
    val writeSupport = new RowWriteSupport
    RowWriteSupport.setSchema(attributes, conf)

    new ParquetWriter[Row](new Path(filePath),
      writeSupport,
      codec,
      parquetBlockSize,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_PAGE_SIZE,
      ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
      ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
      ParquetWriter.DEFAULT_WRITER_VERSION,
      conf)
  }

  def writeMetadata(sc: SparkContext, nameNode: String, attributes: Seq[Attribute], folderPath: String): Unit = {
    val metdataRdd = sc.parallelize(List(folderPath), 1)
    metdataRdd.foreach { path =>
      val conf = new Configuration()
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, nameNode)
      ParquetTypesConverter.writeMetaData(attributes, new Path(path), conf)
    }
  }

  def readMetadata(sc: SparkContext, nameNode: String, folederPath: String) = {
    val metdataRdd = sc.parallelize(List(folederPath), 1)
    metdataRdd.map { path =>
      val conf = new Configuration()
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, nameNode)
      ParquetTypesConverter.readMetaData(new Path(nameNode + "/" + path), Option(conf)).getFileMetaData.getKeyValueMetaData.get(RowReadSupport.SPARK_METADATA_KEY)
    }.first
  }

  def getInputSplits(sc: SparkContext, nameNodeUrl: String, path: String, metadata: String) = {
    val parquetFolderPath = nameNodeUrl + "/" + path
    val parquetFolderRddPath: RDD[String] = sc.parallelize(List(parquetFolderPath), 1)

    val rdd = parquetFolderRddPath.flatMap(path => {
      val fs = FileSystem.newInstance(URI.create(nameNodeUrl), new Configuration())
      val filesList = fs.listFiles(new Path(path), false)
      var pathListResult = List[String]()
      val (conf, job, inputformat) = initializeJob(metadata, nameNodeUrl)

      while (filesList.hasNext()) {
        val fileName = filesList.next().getPath.toString()
        if (fileName.endsWith(".parquet")) {
          pathListResult = pathListResult ::: List(fileName)
          addInputPath(job, new Path(fileName))
        }
      }

      fs.close()
      inputformat.getSplits(job).map(inputSplit => {
        new SerializableWritable(inputSplit.asInstanceOf[InputSplit with Writable])
      })
    })

    rdd.collect.toList
  }

  def sanitizePath(path: String) = {
    var pathIntermediar = if (path.endsWith("/")) path.substring(0, path.size - 1) else path
    if (pathIntermediar.startsWith("/"))
      pathIntermediar.substring(1, pathIntermediar.size)
    else pathIntermediar
  }

  def addInputPath(job: Job, path: Path) = {
    val conf = job.getConfiguration()
    val dirStr = StringUtils.escapeString(path.toString())
    val dirs = conf.get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR)
    conf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, if (dirs == null) dirStr else dirs + "," + dirStr)
  }

  val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  def initializeJob(metadata: String, namenode: String): Tuple3[Configuration, Job, ParquetInputFormat[Row]] = {
    val objectAttributes = ParquetTypesConverter.convertFromString(metadata)
    val jobConf = SparkHadoopUtil.get.newConfiguration()

    val job = new Job(jobConf)
    ParquetInputFormat.setReadSupportClass(job, classOf[RowReadSupport])

    val conf: Configuration = ContextUtil.getConfiguration(job)
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, namenode)
    conf.set(
      RowReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      ParquetTypesConverter.convertToString(objectAttributes))

    val inputFormat = classOf[ParquetInputFormat[Row]].newInstance
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }

    (conf, job, inputFormat)
  }
}