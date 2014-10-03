package com.xpatterns.jaws.data.impl

import com.xpatterns.jaws.data.contracts.TJawsLogging
import com.xpatterns.jaws.data.utils.Utils
import me.prettyprint.hector.api.Keyspace
import com.xpatterns.jaws.data.contracts.TJawsResults
import org.apache.log4j.Logger
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.cassandra.serializers.IntegerSerializer
import me.prettyprint.hector.api.Serializer
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.beans.HColumn
import me.prettyprint.hector.api.beans.ColumnSlice
import me.prettyprint.hector.api.query.QueryResult
import com.xpatterns.jaws.data.DTO.Result
import net.liftweb.json.DefaultFormats
import com.xpatterns.jaws.data.DTO.QueryMetaInfo
import net.liftweb.json._
import spray.json._

class JawsCassandraResults(keyspace: Keyspace) extends TJawsResults {

  val CF_SPARK_RESULTS = "results"
  val CF_SPARK_RESULTS_NUMBER_OF_ROWS = 100

  val logger = Logger.getLogger("JawsCassandraResults")

  val is = IntegerSerializer.get.asInstanceOf[Serializer[Int]]
  val ss = StringSerializer.get.asInstanceOf[Serializer[String]]

 override def getResults(uuid: String): Result = {
   Utils.TryWithRetry {

     logger.debug("Reading results for query: " + uuid)

     val key = computeRowKey(uuid)

     val sliceQuery = HFactory.createSliceQuery(keyspace, is, ss, ss)
     sliceQuery.setColumnFamily(CF_SPARK_RESULTS).setKey(key).setRange(uuid, uuid, false, 1)

     val result = sliceQuery.execute()
     Option(result) match {
       case None => return null
       case _ => {
         val columnSlice = result.get()
         Option(columnSlice) match {
           case None => return null
           case _ => {
             Option(columnSlice.getColumns()) match {
               case None => return null
               case _ => {

                 if (columnSlice.getColumns().size() == 0) {
                   return null
                 }

                 val col = columnSlice.getColumns().get(0)
                 implicit val formats = DefaultFormats
                 val json = parse(col.getValue())
                 return json.extract[Result]

               }
             }
           }
         }

       }
     }
   }
  }

  override def setResults(uuid: String, resultDTO: Result) {
    Utils.TryWithRetry {

      logger.debug("Writing results to query " + uuid)

      val key = computeRowKey(uuid)

      val buffer = resultDTO.toJson.toString()

      val mutator = HFactory.createMutator(keyspace, is)
      mutator.addInsertion(key, CF_SPARK_RESULTS, HFactory.createColumn(uuid, buffer, ss, StringSerializer.get.asInstanceOf[Serializer[Object]]))
      mutator.execute()
    }
  }

  private def computeRowKey(uuid: String): Integer = {
    Math.abs(uuid.hashCode() % CF_SPARK_RESULTS_NUMBER_OF_ROWS)
  }
}
