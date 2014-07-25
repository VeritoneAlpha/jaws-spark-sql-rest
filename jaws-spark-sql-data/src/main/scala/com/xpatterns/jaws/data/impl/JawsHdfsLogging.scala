package com.xpatterns.jaws.data.impl

import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.DTO.Logs
import java.util.Comparator
import java.util.SortedSet
import com.xpatterns.jaws.data.DTO.Log
import com.xpatterns.jaws.data.DTO.Queries
import com.xpatterns.jaws.data.DTO.Query
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import net.liftweb.json._
import spray.json._
import com.xpatterns.jaws.data.utils.QueryState
import com.xpatterns.jaws.data.utils.Utils

class JawsHdfsLogging(configuration: Configuration) extends TJawsLogging {

  val QUERYID_SEPARATOR = "-----";

  val logger = Logger.getLogger("JawsHdfsLogging");

  val forcedMode = configuration.getBoolean(Utils.FORCED_MODE, false);
  Utils.createFolderIfDoesntExist(configuration, configuration.get(Utils.LOGGING_FOLDER), forcedMode);
  Utils.createFolderIfDoesntExist(configuration, configuration.get(Utils.STATUS_FOLDER), forcedMode);
  Utils.createFolderIfDoesntExist(configuration, configuration.get(Utils.DETAILS_FOLDER), forcedMode);
  Utils.createFolderIfDoesntExist(configuration, configuration.get(Utils.METAINFO_FOLDER), forcedMode);

  override def setState(uuid: String, queryState: QueryState.QueryState) {

    logger.debug("Writing query state " + queryState.toString() + " to query " + uuid)
    Utils.rewriteFile(queryState.toString(), configuration, configuration.get(Utils.STATUS_FOLDER) + "/" + uuid);

  }

  override def setScriptDetails(uuid: String, scriptDetails: String) {

    logger.debug("Writing script details " + scriptDetails + " to query " + uuid);
    Utils.rewriteFile(scriptDetails, configuration, configuration.get(Utils.DETAILS_FOLDER) + "/" + uuid);

  }

  override def addLog(uuid: String, queryId: String, time: Long, log: String) {

    logger.debug("Writing log " + log + " to query " + uuid + " at time " + time);

    logger.debug("Writing log " + log + " to query " + uuid + " at time " + time);
    val folderName = configuration.get(Utils.LOGGING_FOLDER) + "/" + uuid;
    val fileName = folderName + "/" + time.toString();
    val logMessage = queryId + QUERYID_SEPARATOR + log;
    Utils.createFolderIfDoesntExist(configuration, folderName, false);
    Utils.rewriteFile(logMessage, configuration, fileName);

  }

  override def getState(uuid: String): QueryState.QueryState = {

    logger.debug("Reading query state for query: " + uuid);
    val state = Utils.readFile(configuration, configuration.get(Utils.STATUS_FOLDER) + "/" + uuid);
    return QueryState.withName(state)
  }

  override def getScriptDetails(uuid: String): String = {
    logger.info("Reading script details for query: " + uuid);
    return Utils.readFile(configuration, configuration.get(Utils.DETAILS_FOLDER) + "/" + uuid);

  }

  override def getLogs(uuid: String, time: Long, limit: Int): Logs = {

    logger.debug("Reading logs for query: " + uuid + " from date: " + time);

    val folderName = configuration.get(Utils.LOGGING_FOLDER) + "/" + uuid;
    var logs = Array[Log]()
    var files = Utils.listFiles(configuration, folderName, new Comparator[String]() {

      override def compare(o1: String, o2: String): Int = {
        return o1.compareTo(o2);
      }

    });

    if (files.contains(time.toString())) {
      files = files.tailSet(time.toString());
    }

    val filesToBeRead = getSubset(limit, files);

    filesToBeRead.foreach(file => {
      val logedInfo = Utils.readFile(configuration, folderName + "/" + file).split(QUERYID_SEPARATOR);
      if (logedInfo.length == 2) {
        logs = logs ++ Array(new Log(logedInfo(1), logedInfo(0), file.asInstanceOf[Long]))

      }
    })

    return new Logs(logs, getState(uuid).toString)
  }

  def getSubset(limit: Int, files: SortedSet[String]): List[String] = {
    var filesToBeRead = List[String]();
    var limitMutable = limit

    val iterator = files.iterator();
    while (iterator.hasNext() && limitMutable > 0) {
      val file = iterator.next();
      filesToBeRead = filesToBeRead ++ List(file)
      limitMutable = limitMutable - 1
    }

    return filesToBeRead;
  }

  override def getQueriesStates(uuid: String, limit: Int): Queries = {

    logger.info("Reading states for queries starting with the query: " + uuid);
    var stateList = Array[Query]()

    val folderName = configuration.get(Utils.STATUS_FOLDER);
    val startFilename = folderName + "/" + uuid;
    var files = Utils.listFiles(configuration, folderName, new Comparator[String]() {

      override def compare(o1: String, o2: String): Int = {
        return o1.compareTo(o2);
      }

    });

    if (files.contains(startFilename)) {
      files = files.tailSet(startFilename);
    }

    val filesToBeRead = getSubset(limit, files);

    filesToBeRead.foreach(file => {
      val currentUuid = Utils.getNameFromPath(file)
      stateList = stateList ++ Array(new Query(Utils.readFile(configuration, folderName + "/" + file), currentUuid, getScriptDetails(currentUuid)))
    })

    return new Queries(stateList)
  }

  override def setMetaInfo(uuid: String, metainfo: QueryMetaInfo) {
    logger.debug("Writing script meta info " + metainfo + " to query " + uuid)
    val buffer = metainfo.toJson.toString
    Utils.rewriteFile(buffer, configuration, configuration.get(Utils.METAINFO_FOLDER) + "/" + uuid);
  }

  override def getMetaInfo(uuid: String): QueryMetaInfo = {
    logger.debug("Reading meta info for for query: " + uuid);

    val value = Utils.readFile(configuration, configuration.get(Utils.METAINFO_FOLDER) + "/" + uuid);

    implicit val formats = DefaultFormats

    val json = parse(value)
    return json.extract[QueryMetaInfo]

  }
}
