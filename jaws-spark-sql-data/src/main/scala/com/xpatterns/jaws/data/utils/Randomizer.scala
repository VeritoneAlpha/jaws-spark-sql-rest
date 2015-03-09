package com.xpatterns.jaws.data.utils

import java.util.ArrayList
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.lang.math.RandomUtils
import com.xpatterns.jaws.data.DTO.Result
import com.xpatterns.jaws.data.DTO.Column
import com.xpatterns.jaws.data.DTO.Log
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import com.xpatterns.jaws.data.DTO.ParquetTable


object Randomizer {

	def  getRandomString(nr : Int) : String = {
		return RandomStringUtils.randomAlphabetic(nr)
	}

	def getRandomLong : Long = {
		return RandomUtils.nextLong()
	}

	
	def getParquetTable : ParquetTable ={
	   new ParquetTable(Randomizer.getRandomString(5), Randomizer.getRandomString(5))
	}
	
	def getParquetTables (size : Int): Array[ParquetTable] = {
		val result : Array[ParquetTable] = new Array(size)	
	  for (i <- 0 until size){
			  result(i) = getParquetTable
			}
		result
	}
	
	def getResult : Result = {
		
		var schema = Array[Column]()
		for (i <- 0 to 10) {
			schema = schema ++ Array(new Column(RandomStringUtils.randomAlphabetic(10) , RandomStringUtils.randomAlphabetic(10)))
		 
		}

		var results = Array[Array[String]]()
		for (i <- 0 to 10) {
			var row = Array [String]()
			for (j <- 0 to 10) {
				row = row ++ Array (RandomStringUtils.randomAlphabetic(10))
			}
			results = results ++ Array(row)
		}
		return new Result(schema, results)
	}

	def getLogDTO: Log = {
		return new Log(Randomizer.getRandomString(5000), Randomizer.getRandomString(10), Randomizer.getRandomLong)
	}

	def createQueryMetainfo  : QueryMetaInfo =  {
		return new QueryMetaInfo(RandomUtils.nextLong(), RandomUtils.nextLong(), RandomUtils.nextInt(3), RandomUtils.nextBoolean())
	}
}
