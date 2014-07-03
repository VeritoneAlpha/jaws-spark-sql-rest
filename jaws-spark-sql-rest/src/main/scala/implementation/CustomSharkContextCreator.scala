package implementation

import org.apache.spark.scheduler.CustomJobLogger
import org.apache.spark.scheduler.LoggingListener
import org.apache.spark.scheduler.SharkUtils

import actors.Configuration
import actors.MainActors
import actors.Systems
import shark.SharkContext
import shark.SharkEnv
import traits.CustomSharkContext
import traits.DAL

/**
 * Created by emaorhian
 */
class CustomSharkContextCreator(dals: DAL) extends CustomSharkContext with MainActors with Systems {
  val jars = Array(Configuration.jarPath.get)

  val sharkContext: SharkContext = {

    var shContext = new SharkContext(Configuration.sparkMaster.get, Configuration.applicationName.getOrElse("Jaws"), Configuration.sparkPath.get, jars.toSeq, Map.empty)
    shContext = SharkEnv.initWithSharkContext(shContext)
    shContext.addSparkListener(new LoggingListener(dals))
    shContext.addSparkListener(new CustomJobLogger(dals))

    SharkUtils.setSharkProperties(shContext, this.getClass().getClassLoader().getResourceAsStream("sharkSettings.txt"))

    shContext
  }

}
