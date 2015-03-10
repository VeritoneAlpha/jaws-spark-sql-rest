package com.xpatterns.jaws.data.utils

import me.prettyprint.hector.api.exceptions.{ HectorException, HUnavailableException, HTimedOutException }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import java.io.BufferedInputStream
import org.apache.hadoop.io.IOUtils
import java.io.InputStream
import java.io.ByteArrayInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.SortedSet
import java.util.Comparator
import java.util.TreeSet

class Utils {}

object Utils {
  val NAMENODE = "namenode"
  val FORCED_MODE = "forcedMode"
  val LOGGING_FOLDER = "loggingFolder"
  val STATUS_FOLDER = "stateFolder"
  val METAINFO_FOLDER = "metaInfoFolder"
  val DETAILS_FOLDER = "detailsFolder"
  val RESULTS_FOLDER = "resultsFolder"
  val SCHEMA_FOLDER = "schemaFolder"
  val REPLICATION_FACTOR = "replicationFactor"

  private val log = Logger.getLogger("Utils")

  def createFolderIfDoesntExist(configuration: Configuration, folder: String, forcedMode: Boolean) {
    log.info("[createFolderIfDoesntExist] forcedMode: " + forcedMode)

    var fs: FileSystem = null
    try {
      fs = FileSystem.newInstance(configuration)
      val path = new Path(folder)
      if (fs.exists(path)) {
        log.info("[createFolderIfDoesntExist] The folder " + folder + " exists!")
        if (forcedMode) {
          log.info("[createFolderIfDoesntExist] We are in forced mode so the folder will be recreated")
          // Delete folder
          fs.delete(path, true)
          createFolder(fs, folder)
        } else {
          log.info("[createFolderIfDoesntExist] We won't recreate the folder!")
        }
      } else {
        createFolder(fs, folder)
      }

    } catch {
      case e: Exception => {
        log.error(e.getMessage())
        throw e
      }
    } finally {
      if (fs != null) {
        fs.close()
      }

    }
  }

  def createFolder(fs: FileSystem, path: String) {
    val temPath = new Path(path)
    if (!fs.exists(temPath))
      fs.mkdirs(temPath)
  }

  def rewriteFile(message: String, configuration: Configuration, filename: String) {
    var fs: FileSystem = null
    val in: InputStream = new BufferedInputStream(new ByteArrayInputStream(message.getBytes()))
    val temporaryFileName = filename + "_temp"

    try {
      // failover mechanism
      val file = new Path(filename)
      val temporaryFile = new Path(temporaryFileName)

      fs = FileSystem.newInstance(configuration)
      val fileOutputStream = fs.create(temporaryFile, true)

      // write into temporary file
      IOUtils.copyBytes(in, fileOutputStream, configuration, true)

      // delete the old file and rename the temporary file
      if (fs.exists(file)) {
        fs.delete(file, true)
      }
      fs.rename(temporaryFile, file)
    } catch {
      case ex: Exception => {
        log.error(ex.getStackTraceString)
        throw ex
      }
    } finally {
      if (fs != null) {
        fs.close()
      }

    }
  }

  def readFile(configuration: Configuration, filename: String): String = {
    var content = ""
    var br: BufferedReader = null
    var fs: FileSystem = null
    try {
      val filePath = new Path(filename)
      fs = FileSystem.newInstance(configuration)
      br = new BufferedReader(new InputStreamReader(fs.open(filePath)))

      var line = br.readLine()
      while (line != null) {
        content = content + line + "\n"
        line = br.readLine()
      }
      return content.toString().trim()
    } finally {
      if (br != null) {
        br.close()
      }
      if (fs != null) {
        fs.close()
      }
    }

  }

  def deleteFile(configuration: Configuration, filename: String) = {
    var fs: FileSystem = null
    try {
      val filePath = new Path(filename)
      fs = FileSystem.newInstance(configuration)
      if (fs.exists(filePath)) {
        fs.delete(filePath)
      }

    } finally {

      if (fs != null) {
        fs.close()
      }
    }

  }

  def getNameFromPath(path: String): String = {
    var name = ""
    var mutablePath = path
    if (mutablePath.charAt(path.length() - 1) == '/')
      mutablePath = mutablePath.substring(0, mutablePath.length() - 1)

    if (mutablePath.contains("/"))
      name = mutablePath.substring(path.lastIndexOf("/") + 1, mutablePath.length())
    else
      name = mutablePath
    return name
  }

  def listFiles(configuration: Configuration, folderName: String, comparator: Comparator[String]): SortedSet[String] = {
    var fs: FileSystem = null
    val allFiles: SortedSet[String] = new TreeSet[String](comparator)
    try {
      val folderPath = new Path(folderName)
      fs = FileSystem.newInstance(configuration)
      val files = fs.listFiles(folderPath, false)
      while (files.hasNext()) {
        val file = files.next()
        allFiles.add(file.getPath().getName())
      }

      return allFiles

    } finally {
      if (fs != null) {
        fs.close()
      }
    }
  }

  def checkFileExistence(filename: String, configuration: Configuration): Boolean = {
    val file = new Path(filename)
    val fs = FileSystem.newInstance(configuration)

    fs.exists(file)
  }

  def TryWithRetry[A](f: => A): A = {
    val maxRetries = 30
    val sleepRetry = 1000
    val logger = Logger.getLogger(this.getClass().getName())

    var finished: Boolean = false
    var result: A = null.asInstanceOf[A]
    var retries: Integer = 0

    while (!finished) {
      try {
        finished = true
        result = f
      } catch {
        case ex: HTimedOutException =>
          {
            retries += 1
            finished = false
            logger.warn("Retrying " + retries + "/" + maxRetries + " at " + sleepRetry + "ms " + ex.getStackTraceString)
            try {
              Thread.sleep(sleepRetry);
            } catch {
              case ex: InterruptedException => logger.warn(ex.getStackTraceString)
            }
            if (retries > maxRetries)
              throw ex
          }
        case ex: HUnavailableException =>
          {
            retries += 1
            finished = false
            logger.warn("Retrying " + retries + "/" + maxRetries + " at " + sleepRetry + "ms " + ex.getStackTraceString)
            try {
              Thread.sleep(sleepRetry);
            } catch {
              case ex: InterruptedException => logger.warn(ex.getStackTraceString)
            }
            if (retries > maxRetries)
              throw ex
          }
        case ex: HectorException => {
          if (ex.getMessage().contains("All host pools marked down. Retry burden pushed out to client.")) {
            retries += 1
            finished = false
            logger.warn("Retrying " + retries + "/" + maxRetries + " at " + sleepRetry + "ms " + ex.getMessage() + " ")
            try {
              Thread.sleep(sleepRetry);
            } catch {
              case ex: InterruptedException => logger.warn(ex.getMessage())
            }
            if (retries > maxRetries)
              throw ex
          } else
            throw ex;
        }
      }
    }
    result
  }

}