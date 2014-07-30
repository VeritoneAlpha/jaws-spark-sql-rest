package implementation

import org.apache.spark.scheduler.LoggingListener
import actors.Configuration
import actors.MainActors
import actors.Systems
import traits.DAL
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.HiveUtils

/**
 * Created by emaorhian
 */
class CustomHiveContextCreator(dals: DAL) extends MainActors with Systems {
  val jars = Array(Configuration.jarPath.get)

  val hiveContext: HiveContext = {

    var sContext = new SparkContext(Configuration.sparkMaster.get, Configuration.applicationName.getOrElse("Jaws"), Configuration.sparkPath.get, jars.toSeq, Map.empty)
//    sContext.addSparkListener(new LoggingListener(dals))

    var hiveContext = new HiveContext(sContext)
    hiveContext.sparkContext.addSparkListener(new LoggingListener(dals))

    HiveUtils.setSharkProperties(hiveContext, this.getClass().getClassLoader().getResourceAsStream("sharkSettings.txt"))
     
    hiveContext
  }

}
