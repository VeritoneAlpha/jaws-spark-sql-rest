package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsParquetTables
import spray.json._
import spray.json.DefaultJsonProtocol._
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
    val valueTouple = (pTable.namenode, pTable.filePath).toJson.prettyPrint
    Utils.rewriteFile(valueTouple, configuration, getParquetTableFilePath(pTable.name))
  }

  override def deleteParquetTable(name: String) {
    logger.debug(s"Deleting parquet table called $name")
    val filePath = getParquetTableFilePath(name)
    Utils.deleteFile(configuration, filePath)
  }

  override def listParquetTables(): Array[ParquetTable] = {

    logger.debug("Listing parquet tables: ")
    var tables = Array[ParquetTable]()

    val files = Utils.listFiles(configuration, Utils.PARQUET_TABLES_FOLDER, new Comparator[String]() {

      override def compare(o1: String, o2: String): Int = {
        o1.compareTo(o2)
      }

    })

    val iterator = files.iterator()

    while (iterator.hasNext) {
      val tableName = iterator.next()
      
      val (namenode, filepath) = Utils.readFile(configuration, Utils.PARQUET_TABLES_FOLDER + "/" +
                                    tableName).parseJson.convertTo[(String, String)]
      tables = tables :+ new ParquetTable(tableName, filepath, namenode) 
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

    if (Utils.checkFileExistence(filename, configuration)){
      val (namenode, filepath) = Utils.readFile(configuration, filename).parseJson.convertTo[(String, String)]
      new ParquetTable(name, filepath, namenode)
    }
      
    else new ParquetTable

  }

  def getParquetTableFilePath(name: String): String = {
    configuration.get(Utils.PARQUET_TABLES_FOLDER) + "/" + name
  }
}