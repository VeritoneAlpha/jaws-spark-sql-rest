package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsParquetTables
import com.xpatterns.jaws.data.DTO.ParquetTable
import org.apache.hadoop.conf.Configuration
import com.xpatterns.jaws.data.utils.Utils
import org.apache.log4j.Logger
import java.util.Comparator

class JawsHdfsParquetTables(configuration: Configuration) extends TJawsParquetTables {

  val logger = Logger.getLogger("JawsHdfsParquetTables")

  val forcedMode = configuration.getBoolean(Utils.FORCED_MODE, false)
  Utils.createFolderIfDoesntExist(configuration, configuration.get(Utils.PARQUET_TABLES_FOLDER), forcedMode)

  override def addParquetTable(pTable: ParquetTable) {
    logger.debug(s"Writing parquet table ${pTable.name} with path ${pTable.filePath} ")
    Utils.rewriteFile(pTable.filePath, configuration, getParquetTableFilePath(pTable.name))
  }

  override def deleteParquetTable(name: String) {
    logger.debug(s"Deleting parquet table called $name")
    var filePath = getParquetTableFilePath(name)
    Utils.deleteFile(configuration, filePath)
  }

  override def listParquetTables(): Array[ParquetTable] = {

    logger.debug("Listing parquet tables: ")
    var tables = Array[ParquetTable]()

    var files = Utils.listFiles(configuration, Utils.PARQUET_TABLES_FOLDER, new Comparator[String]() {

      override def compare(o1: String, o2: String): Int = {
        return o1.compareTo(o2)
      }

    })

    val iterator = files.iterator()

    while (iterator.hasNext()) {
      val tableName = iterator.next()
      tables = tables :+ new ParquetTable(tableName, Utils.readFile(configuration, Utils.PARQUET_TABLES_FOLDER + "/" + tableName)) 
    }
    
    tables
  }

  override def tableExists(name: String): Boolean = {
    logger.debug(s"Checking table existence for $name")
    val filename = getParquetTableFilePath(name)

    Utils.checkFileExistence(filename, configuration)
  }

  override def readParquetTable(name: String): ParquetTable = {
    logger.debug(s"Reading table $name")
    val filename = getParquetTableFilePath(name)

    if (Utils.checkFileExistence(filename, configuration))
      new ParquetTable(name, Utils.readFile(configuration, filename))
    else new ParquetTable

  }

  def getParquetTableFilePath(name: String): String = {
    configuration.get(Utils.PARQUET_TABLES_FOLDER) + "/" + name
  }
}