package utils

import org.apache.log4j.Logger
import server.JawsController
import java.text.SimpleDateFormat
import java.util.Date

object TestUtils {
  import com.typesafe.config.ConfigFactory

  
   val timestamp = new SimpleDateFormat("yyyyMMdd-HHmmss")

  def getWarehousePath(prefix: String): String = {
    System.getProperty("user.dir") + "/test_warehouses/" + prefix + "-warehouse-" +
      timestamp.format(new Date)
  }

  def getMetastorePath(prefix: String): String = {
    System.getProperty("user.dir") + "/test_warehouses/" + prefix + "-metastore-" +
      timestamp.format(new Date)
  }
  
  val log4j = Logger.getLogger(TestUtils.getClass())

  private val conf = ConfigFactory.load
  conf.checkValid(ConfigFactory.defaultReference)

  val test = conf.getConfig("test").withFallback(conf)
   val dataFilePath = System.getProperty("user.dir") + Option(test.getString("dataFilePath")).getOrElse("") + "/data"
}