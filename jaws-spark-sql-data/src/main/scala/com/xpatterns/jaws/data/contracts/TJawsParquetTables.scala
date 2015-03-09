package com.xpatterns.jaws.data.contracts

import com.xpatterns.jaws.data.DTO.ParquetTable

trait TJawsParquetTables {
	def addParquetTable(pTable : ParquetTable)
	def deleteParquetTable(name : String)
	def listParquetTables() : Array[ParquetTable]
	def tableExists(name : String) : Boolean
	def readParquetTable(name : String) : ParquetTable
}