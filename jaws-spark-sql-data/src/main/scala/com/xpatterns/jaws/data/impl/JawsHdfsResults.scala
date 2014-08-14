package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsResults
import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration
import com.xpatterns.jaws.data.DTO.ResultDTO
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

  override def setResults(uuid: String, resultDTO: ResultDTO) {
	  logger.debug("Writing results to query " + uuid)
	  Utils.rewriteFile(resultDTO.toJson.toString, configuration, configuration.get(Utils.RESULTS_FOLDER) + "/" + uuid)
  }

  override def getResults(uuid: String): ResultDTO = {
    logger.debug("Reading results for query: " + uuid)
    implicit val formats = DefaultFormats

    val resultsString = Utils.readFile(configuration, configuration.get(Utils.RESULTS_FOLDER) + "/" + uuid)
    val json = parse(resultsString)
    return json.extract[ResultDTO]

  }
}