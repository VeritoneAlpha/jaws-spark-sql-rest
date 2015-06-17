package com.xpatterns.jaws.data.utils

import java.util.ArrayList
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.lang.math.RandomUtils
import com.xpatterns.jaws.data.DTO.Column
import com.xpatterns.jaws.data.DTO.Log
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import com.xpatterns.jaws.data.DTO.ParquetTable
import com.xpatterns.jaws.data.DTO.AvroResult
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.types._



object Randomizer {

	def  getRandomString(nr : Int) : String = {
		return RandomStringUtils.randomAlphabetic(nr)
	}

	def getRandomLong : Long = {
		return RandomUtils.nextLong()
	}

	
	def getParquetTable : ParquetTable ={
	   new ParquetTable(Randomizer.getRandomString(5), Randomizer.getRandomString(5), Randomizer.getRandomString(5))
	}
	
	def getParquetTables (size : Int): Array[ParquetTable] = {
		val result : Array[ParquetTable] = new Array(size)	
	  for (i <- 0 until size){
			  result(i) = getParquetTable
			}
		result
	}
	
  def getResultsConverter : ResultsConverter = {
    
  val intField = new StructField("int", IntegerType, false)
  val strField = new StructField("str", StringType, true)
  val structType = new StructType(Array(intField, strField))

  val structTypeRow = Array(Row.fromSeq(Seq(1, "a")), Row.fromSeq(Seq(2, "b")))
  new ResultsConverter(structType, structTypeRow)
 
  }

	def getLogDTO: Log = {
		return new Log(Randomizer.getRandomString(5000), Randomizer.getRandomString(10), Randomizer.getRandomLong)
	}

	def createQueryMetainfo  : QueryMetaInfo =  {
		return new QueryMetaInfo(RandomUtils.nextLong(), RandomUtils.nextLong(), RandomUtils.nextInt(3), RandomUtils.nextBoolean())
	}
}
