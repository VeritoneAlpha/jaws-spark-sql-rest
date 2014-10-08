package customs
import scala.Array.canBuildFrom
import scala.Iterator
import org.apache.spark.rdd.RDD
import server.Configuration

/**
 * Created by emaorhian
 */
class CustomIndexer {

  def indexRdd(rdd: RDD[Array[String]]): RDD[Tuple2[Long, Array[String]]] = {
    val partitionCount = rdd.mapPartitionsWithIndex { (pid, iter) => Iterator((pid, iter.size)) }.collect

    var numberOfPartitions = partitionCount.size
    var sizes = Array[Int]()
    var indexes = Array[Int](0)
    Configuration.log4j.debug("NumberOfPartitions is: " + numberOfPartitions)

    //create the sizes array
    for (a <- partitionCount) {
      sizes = sizes :+ a._2
    }

    //create the indexes array
    for (i <- 0 to numberOfPartitions - 2) {
      indexes = indexes :+ sizes(i)
    }

    val broadcastedIndexes = rdd.sparkContext.broadcast(indexes)

    //index each row
    val indexedRdd = rdd.mapPartitionsWithIndex { (index, iterator) =>
      var z = Array[Tuple2[Long, Array[String]]]()
      var startIndex: Long = broadcastedIndexes.value(index)
      for (element <- iterator) {
        z = z ++ Array((startIndex, element))
        startIndex = startIndex + 1
      }
      z.iterator
    }

    return indexedRdd

  }

}