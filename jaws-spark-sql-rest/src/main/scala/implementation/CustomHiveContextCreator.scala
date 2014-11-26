package implementation

import org.apache.spark.scheduler.LoggingListener
import scala.collection.JavaConverters._
import server.Configuration
import server.MainActors
import traits.DAL
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.HiveUtils
import org.apache.spark.SparkConf
import com.typesafe.config.Config

/**
 * Created by emaorhian
 */
class CustomHiveContextCreator(dals: DAL) {
  val jars = Array(Configuration.jarPath.get)

  val hiveContext: HiveContextWrapper = {

    var sparkConf = configToSparkConf(Configuration.sparkConf, Configuration.applicationName.getOrElse("Jaws"), jars)
    var sContext = new SparkContext(sparkConf)
    
    var hiveContext = new HiveContextWrapper(sContext)
    hiveContext.sparkContext.addSparkListener(new LoggingListener(dals))
    
    HiveUtils.setSharkProperties(hiveContext, this.getClass().getClassLoader().getResourceAsStream("sharkSettings.txt"))
    //make sure that lazy variable hiveConf gets initialized
    hiveContext.runMetadataSql("use default")
    hiveContext
  }

  
   def configToSparkConf(config: Config, contextName: String, jars: Array[String]): SparkConf = {
    val sparkConf = new SparkConf().setAppName(contextName).setJars(jars)
    for (
      property <- config.entrySet().asScala if (property.getKey.startsWith("spark") && property.getValue() != null)
    ) {
      val key=property.getKey().replaceAll("-", ".");
      println(key + " | " + property.getValue.unwrapped())
      sparkConf.set(key, property.getValue.unwrapped().toString)
    }
    sparkConf
  }
}
