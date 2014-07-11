package model
import spray.json.DefaultJsonProtocol._


/**
 * Created by emaorhian
 */

case class QueryInfo (state: String, queryID: String, query: String){
}

object QueryInfo {
  implicit val queryInfoJson = jsonFormat3(apply)  
}