package api

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import foundation.TestBase
import com.xpatterns.jaws.data.utils.Utils
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FileSystem
import java.io.File
import org.apache.hadoop.fs.Path
import akka.io.IO
import akka.pattern.ask
import spray.can.Http
import spray.http._
import spray.client.pipelining._
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.duration.Duration._
import scala.util.Success
import scala.util.Failure
import scala.collection.GenSeq
import scala.concurrent._
import ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class ParquetManagementApiTest extends TestBase {

  override def beforeAll() {
    println("creating parquet folder on hdfs")
    Utils.createFolderIfDoesntExist(hadoopConf, parquetFolder, true)
    val fs = FileSystem.newInstance(hadoopConf)
    val metadataFile = new File(getClass().getResource("/jawsTest.parquet/_metadata").getPath())
    val dataFile = new File(getClass().getResource("/jawsTest.parquet/part-r-1.parquet").getPath())
    FileUtil.copy(metadataFile, fs, new Path(parquetFolder), false, hadoopConf)
    FileUtil.copy(dataFile, fs, new Path(parquetFolder), false, hadoopConf)
  }

  test(" register test table ") {

    val username = System.getProperties().get("user.name")
    val url = s"${jawsUrl}parquet/tables?path=hdfs://$namenodeIp:8020/user/$username/$parquetFolder/&name=$parquetTable&overwrite=true"

    val postResult = post(url, "")

   Await.ready(postResult, Inf).value.get match {
      case Success(r: HttpResponse) => {
        assert(r.status.isSuccess)
        assert(r.entity.data.asString.equals(s"Table $parquetTable was registered"))
      }
      case Failure(e) => {
        println(e.getMessage)
        fail()
      }

    }
  }
  
  test(" register test table overwrite false ") {

    val username = System.getProperties().get("user.name")
    val url = s"${jawsUrl}parquet/tables?path=hdfs://$namenodeIp:8020/user/$username/$parquetFolder/&name=$parquetTable&overwrite=false"

    val postResult = post(url, "")

   Await.ready(postResult, Inf).value.get match {
      case Success(r: HttpResponse) => {
        assert(r.status.isFailure)
        assert(r.entity.data.asString.equals(s"The table already exists!"))
      }
      case Failure(e) => {
        println(e.getMessage)
        fail()
      }

    }
  }
  
  test(" select * from parquet table ") {
    
    val url = s"${jawsUrl}run?limited=true"
    val queryID = selectAllFromTable(url, parquetTable)
    validataAllResultsFromParquetTable(queryID)

  }
  
   test(" unregister test table ") {

    val username = System.getProperties().get("user.name")
    val url = s"${jawsUrl}parquet/tables/$parquetTable"

    val deleteResult = delete(url)

   Await.ready(deleteResult, Inf).value.get match {
      case Success(r: HttpResponse) => {
        assert(r.status.isSuccess)
        assert(r.entity.data.asString.equals(s"Table $parquetTable was unregistered"))
      }
      case Failure(e) => {
        println(e.getMessage)
        fail()
      }

    }
  }
}