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

    var indexes = Array[Int](0)
    Configuration.log4j.debug("NumberOfPartitions is: " + partitionCount.size)

    val resultsNumber = partitionCount.foldLeft(0)((sizeSum, partInfo) => {
      indexes = indexes :+ (sizeSum + partInfo._2)
      sizeSum + partInfo._2
    })

    Configuration.log4j.debug("Number of results is: " + resultsNumber)

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