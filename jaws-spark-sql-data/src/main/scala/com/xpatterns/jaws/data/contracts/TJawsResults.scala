package com.xpatterns.jaws.data.contracts

import com.xpatterns.jaws.data.utils.Utils
import com.xpatterns.jaws.data.utils.ResultsConverter
import com.xpatterns.jaws.data.DTO.AvroResult
import com.xpatterns.jaws.data.DTO.CustomResult

/**
 * Created by emaorhian
 */
trait TJawsResults {
  def setAvroResults (uuid: String, avroResults : AvroResult)
  def getAvroResults(uuid: String) : AvroResult
  def setCustomResults(uuid: String, results: CustomResult)
  def getCustomResults(uuid: String): CustomResult
  
  def setResults(uuid: String, results: ResultsConverter) {
    Utils.TryWithRetry { 
      
      setAvroResults(uuid, results.toAvroResults())
      setCustomResults(uuid, results.toCustomResults)
    }
  }
  def deleteResults(uuid: String)
}