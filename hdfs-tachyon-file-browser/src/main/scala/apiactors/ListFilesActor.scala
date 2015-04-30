package apiactors

import server.{ Configuration => FileBrowserConfiguration }
import akka.actor.Actor
import com.google.common.base.Preconditions
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import java.util.regex.Pattern
import java.util.regex.Matcher
import org.apache.hadoop.conf.Configuration
import scala.collection.immutable.SortedSet
import java.util.Comparator
import org.apache.hadoop.fs.FileSystem
import scala.collection.immutable.TreeSet
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil
import customs.Files
import customs.File
/**
 * Created by emaorhian
 */

case class ListFilesMessage(route: String)
case class ErrorMessage(message: String)

class ListFilesActor extends Actor {

  override def receive = {

    case message: ListFilesMessage => {
      FileBrowserConfiguration.log4j.info(s"[ListFilesActor]: listing files in ${message.route}")
      
      val tryGetQueryInfo = Try {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        val (namenode, path) = splitPath(message.route)
        val configuration = new org.apache.hadoop.conf.Configuration()
        configuration.set("fs.defaultFS", namenode)
        configuration.set("fs.tachyon.impl","tachyon.hadoop.TFS")
        Files(path , listFiles(configuration, path))
      }

      tryGetQueryInfo match {
        case Success(result) => sender ! result
        case Failure(e) => sender ! ErrorMessage(s"List files failed with the following message: ${e.getMessage}")
      }
    }
  }

  def listFiles(configuration: Configuration, folderName: String): Array[File] = {
    var fs: FileSystem = null
    
    try {
      val folderPath = new Path(folderName)
      fs = FileSystem.newInstance(configuration)
      if (fs.isDirectory(folderPath)) {
        val files = fs.listStatus(folderPath)
        files map { fstatus => new File(fstatus.getPath.getName, fstatus.isDirectory())} 
      
      }
      else Array[File]()
    } finally {
      if (fs != null) {
        fs.close()
      }
    }
  }

  val pattern: Pattern = Pattern.compile("^([^/]+://[^/]+)(.+?)/*$")
  def splitPath(filePath: String): Tuple2[String, String] = {
    val matcher: Matcher = pattern.matcher(filePath)

    if (matcher.matches())
      (matcher.group(1), matcher.group(2))
    else
      throw new Exception(s"Invalid file path format : $filePath")

  }
}
