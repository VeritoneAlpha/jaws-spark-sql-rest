package com.xpatterns.jaws.data.contracts

/**
 * Created by emaorhian
 */
trait DAL {

  def loggingDal : TJawsLogging
  def resultsDal : TJawsResults
  def parquetTableDal : TJawsParquetTables
}