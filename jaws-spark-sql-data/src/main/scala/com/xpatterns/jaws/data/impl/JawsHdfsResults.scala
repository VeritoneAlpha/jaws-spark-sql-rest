package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsResults
import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration
import com.xpatterns.jaws.data.DTO.Result
import net.liftweb.json._
import spray.json._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.fs.FileSystem
import java.io.InputStream
import org.apache.commons.io.output.ByteArrayOutputStream
import com.xpatterns.jaws.data.utils.Utils

class JawsHdfsResults(configuration: Configuration) extends TJawsResults {

  val logger = Logger.getLogger("JawsHdfsResults")
  val forcedMode = configuration.getBoolean(Utils.FORCED_MODE, false)
  Utils.createFolderIfDoesntExist(configuration, configuration.get(Utils.RESULTS_FOLDER), forcedMode)

  def setResults(uuid: String, resultDTO: Result) {
    logger.debug("Writing results to query " + uuid)
    Utils.rewriteFile(resultDTO.toJson.toString, configuration, getResultsFilePath(uuid))
  }

  def getResults(uuid: String): Result = {
    logger.debug("Reading results for query: " + uuid)
    implicit val formats = DefaultFormats
    val filePath = getResultsFilePath(uuid)
    
    if (Utils.checkFileExistence(filePath, configuration)) parse(Utils.readFile(configuration, filePath)).extract[Result] else new Result
  }

  def deleteResults(uuid: String) {
    logger.debug(s"Deleting results for query $uuid")
    val filePath = getResultsFilePath(uuid)
    Utils.deleteFile(configuration, filePath)

  }

  def getResultsFilePath(queryId: String): String = {
    configuration.get(Utils.RESULTS_FOLDER) + "/" + queryId
  }
}