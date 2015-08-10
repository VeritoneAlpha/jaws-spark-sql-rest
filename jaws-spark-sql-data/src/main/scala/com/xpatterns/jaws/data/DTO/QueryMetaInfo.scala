package com.xpatterns.jaws.data.DTO

import org.apache.log4j.Logger
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.collection.mutable


/**
 * Created by emaorhian
 */
case class QueryMetaInfo(var name:Option[String], var description:Option[String], var published:Option[Boolean], var timestamp:Long, var executionTime:Long,
                         var nrOfResults:Long, var maxNrOfResults:Long, var resultsDestination:Int,
                         var isLimited:Boolean){
 // resultsDestination : 0-cassandra, 1-hdfs, 2-tachyon 
   def this() = {
     this(None, None, None, 0, 0, 0, 0, 0, false)
   }

  def this(nrOfResults : Long, maxNrOfResults : Long, resultsDestination : Int, isLimited : Boolean) = {
    this(None, None, None, 0, 0, nrOfResults, maxNrOfResults, resultsDestination, isLimited)
  }
   
}

object QueryMetaInfo {
  val logger = Logger.getLogger("QueryMetaInfo")

  // A custom json format is defined because some fields might be missing.
  implicit val logJson = new RootJsonFormat[QueryMetaInfo] {
    def write(metaInfo: QueryMetaInfo):JsValue = {
      val fields:mutable.Map[String, JsValue] = mutable.Map.empty[String, JsValue]

      // Don't serialize the null values of name and description because this value means that they are deleted.
      val queryHasName = if (metaInfo.name != None && metaInfo.name.get != null) {
        fields("name") = JsString(metaInfo.name.get)
        true
      } else {
        false
      }

      // Write the description or published only when the query has a name
      // to make sure that these properties are not visible
      if (metaInfo.description != None && metaInfo.description.get != null && queryHasName) {
        fields("description") = JsString(metaInfo.description.get)
      }

      if (metaInfo.published != None && metaInfo.name != None && metaInfo.name.get != null && queryHasName) {
        fields("published") = JsBoolean(metaInfo.published.get)
      }

      fields("timestamp") = JsNumber(metaInfo.timestamp)
      fields("executionTime") = JsNumber(metaInfo.executionTime)
      fields("nrOfResults") = JsNumber(metaInfo.nrOfResults)
      fields("maxNrOfResults") = JsNumber(metaInfo.maxNrOfResults)
      fields("resultsDestination") = JsNumber(metaInfo.resultsDestination)
      fields("isLimited") = JsBoolean(metaInfo.isLimited)

      JsObject(fields.toMap)
    }

    def read(value: JsValue):QueryMetaInfo = value match {
      case JsObject(fields) =>
        val name = if (fields.contains("name")) {
          Some(fields.getOrElse("name", JsNull).convertTo[Option[String]].orNull)
        } else {
          None
        }

        val description = if (fields.contains("description")) {
          Some(fields.getOrElse("description", JsNull).convertTo[Option[String]].orNull)
        } else {
          None
        }

        val published = if (fields.contains("published")) {
          fields.getOrElse("published", JsNull).convertTo[Option[Boolean]]
        } else {
          None
        }

        val timestamp = fields.getOrElse("timestamp", JsNumber(0)).convertTo[Long]
        val executionTime = fields.getOrElse("executionTime", JsNumber(0)).convertTo[Long]
        val nrOfResults = fields.getOrElse("nrOfResults", JsNumber(0)).convertTo[Long]
        val maxNrOfResults = fields.getOrElse("maxNrOfResults", JsNumber(0)).convertTo[Long]
        val resultsDestination = fields.getOrElse("resultsDestination", JsNumber(0)).convertTo[Int]
        val isLimited = fields.getOrElse("isLimited", JsFalse).convertTo[Boolean]

        new QueryMetaInfo(name, description, published, timestamp, executionTime, nrOfResults, maxNrOfResults,
          resultsDestination, isLimited)

      case _ => deserializationError("Error while trying to parse a QueryMetaInfo")
    }
  }
}