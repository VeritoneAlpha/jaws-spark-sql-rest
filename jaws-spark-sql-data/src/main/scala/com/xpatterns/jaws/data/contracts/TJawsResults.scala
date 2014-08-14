package com.xpatterns.jaws.data.contracts

import com.xpatterns.jaws.data.DTO.ResultDTO

/**
 * Created by emaorhian
 */
trait TJawsResults {
  def getResults(uuid: String): ResultDTO
  def setResults(uuid: String, resultDTO: ResultDTO)
}