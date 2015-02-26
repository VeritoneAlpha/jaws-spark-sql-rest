package com.xpatterns.jaws.data.contracts

import com.xpatterns.jaws.data.DTO.Result

/**
 * Created by emaorhian
 */
trait TJawsResults {
  def getResults(uuid: String): Result
  def setResults(uuid: String, resultDTO: Result)
  def deleteResults(uuid: String)
}